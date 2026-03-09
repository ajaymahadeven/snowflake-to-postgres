"""
snowflake_to_postgres/management/commands/sf_migrate.py

Django management command for Snowflake to PostgreSQL migration.
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from ...connections import PostgresConnection, SnowflakeConnection
from ...data_transfer import DataTransferEngine
from ...discovery import SnowflakeSchemaDiscovery
from ...executor import PostgresDDLExecutor
from ...translator import PostgresDDLGenerator
from ...validator import DataValidator
from ...view_procedure_translator import (
    SnowflakeProcedureTranslator,
    SnowflakeViewTranslator,
)


class TeeWriter:
    """Writes to the wrapped stdout AND, if a log_file is open, streams to it in real-time."""

    def __init__(self, wrapped, log_file=None):
        self._wrapped = wrapped
        self._log_file = log_file

    def write(self, msg, style_func=None, ending="\n"):
        self._wrapped.write(msg, style_func=style_func, ending=ending)
        if self._log_file:
            clean = re.sub(r"\x1b\[[0-9;]*m", "", msg + ending)
            self._log_file.write(clean)
            self._log_file.flush()

    def flush(self):
        self._wrapped.flush()

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class Command(BaseCommand):
    help = """
Snowflake to PostgreSQL Migration Tool

Commands:
    discover  - Discover and display schema structure
    build     - Create schema structure in PostgreSQL (no data)
    destroy   - Drop schema and all tables
    migrate   - Full migration (build + data transfer)
    transfer  - Transfer data only (schema must exist)

Examples:
    python manage.py sf_migrate discover --schema ENDEAVOUR_STAGING
    python manage.py sf_migrate build --schema ENDEAVOUR_STAGING --target endeavour_staging
    python manage.py sf_migrate destroy --target endeavour_staging
    python manage.py sf_migrate migrate --schema ENDEAVOUR_STAGING --target endeavour_staging
    python manage.py sf_migrate transfer --schema ENDEAVOUR_STAGING --target endeavour_staging --table MY_TABLE
    python manage.py sf_migrate validate --schema ENDEAVOUR_STAGING --target endeavour_staging
    python manage.py sf_migrate validate --schema ENDEAVOUR_STAGING --target endeavour_staging --table MY_TABLE --sample-size 10000
    """

    def add_arguments(self, parser):
        # Subcommands
        parser.add_argument(
            "action",
            type=str,
            choices=[
                "discover",
                "build",
                "build-views",
                "destroy",
                "migrate",
                "transfer",
                "validate",
            ],
            help="Action to perform",
        )

        # Source schema
        parser.add_argument(
            "--schema",
            type=str,
            help="Source Snowflake schema name (e.g., ENDEAVOUR_STAGING)",
        )

        # Target schema
        parser.add_argument(
            "--target",
            type=str,
            help="Target PostgreSQL schema name (defaults to lowercase source schema)",
        )

        # Database alias
        parser.add_argument(
            "--db",
            type=str,
            default="data_factory_ops",
            help="Django database alias for PostgreSQL (default: data_factory_ops)",
        )

        # Specific table
        parser.add_argument(
            "--table", type=str, help="Specific table to process (optional)"
        )

        # Batch size for data transfer
        parser.add_argument(
            "--batch-size",
            type=int,
            default=10000,
            help="Batch size for data transfer (default: 10000)",
        )

        # WHERE clause for filtering data
        parser.add_argument(
            "--where",
            type=str,
            help="SQL WHERE clause to filter rows (e.g., --where \"DATE >= '2025-01-01'\")",
        )

        # LIMIT for testing
        parser.add_argument(
            "--limit",
            type=int,
            help="Limit number of rows to transfer (useful for testing)",
        )

        # Dry run mode
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be done without making changes",
        )

        # Output format
        parser.add_argument(
            "--format",
            type=str,
            choices=["text", "json"],
            default="text",
            help="Output format (default: text)",
        )

        # Output file
        parser.add_argument(
            "--output", type=str, help="Write DDL to file instead of executing"
        )

        # Force (skip confirmations)
        parser.add_argument(
            "--force", action="store_true", help="Skip confirmation prompts"
        )

        # Continue on error
        parser.add_argument(
            "--continue-on-error",
            action="store_true",
            help="Continue processing even if errors occur",
        )

        # Skip post-action interactive prompts (verify / save logs)
        parser.add_argument(
            "--no-prompt",
            action="store_true",
            help="Skip post-action interactive prompts (for automation)",
        )

        # Sample size for validate Layer 5 (row-level comparison)
        parser.add_argument(
            "--sample-size",
            type=int,
            default=0,
            help=(
                "Number of rows to sample for row-level comparison during validate "
                "(Layer 5). Requires a primary key on the source table. "
                "Default: 0 (skipped)."
            ),
        )

    def handle(self, *args, **options):
        action = options["action"]
        start_time = datetime.now()
        suppress = (
            options.get("force") or options.get("no_prompt") or options.get("dry_run")
        )

        # Validate --schema exists in Snowflake before doing anything else
        _schema_actions = {
            "migrate",
            "transfer",
            "build",
            "validate",
            "build-views",
            "discover",
        }
        if action in _schema_actions and options.get("schema"):
            try:
                self._assert_schema_exists(options["schema"])
            except CommandError:
                raise
            except Exception as e:
                raise CommandError(f"Could not verify schema: {e}")

        # Actions that support log saving / verify prompts
        _log_actions = {"migrate", "transfer", "build", "validate", "build-views"}

        log_path = None
        log_file = None
        self._log_folder = None  # shared across all sub-actions in this run

        # Ask about log saving BEFORE the action starts so we can stream output live
        if action in _log_actions and not suppress:
            try:
                answer = input(f"Save {action} log to file? [y/N]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ""
            if answer == "y":
                log_path, log_file = self._open_log_file(options, action, start_time)
                if log_path:
                    self._log_folder = log_path.parent

        tee = TeeWriter(self.stdout, log_file)
        self.stdout = tee

        try:
            if action == "discover":
                self.handle_discover(options)
            elif action == "build":
                self.handle_build(options)
                self._post_action(options, action, verify=False)
            elif action == "build-views":
                self.handle_build_views(options)
                self._post_action(options, action, verify=False)
            elif action == "destroy":
                self.handle_destroy(options)
            elif action == "migrate":
                self.handle_migrate(options)
                self._post_action(options, action, verify=True)
            elif action == "transfer":
                self.handle_transfer(options)
                self._post_action(options, action, verify=True)
            elif action == "validate":
                self.handle_validate(options)
                self._post_action(options, action, verify=False)

        except Exception as e:
            raise CommandError(f"Error during {action}: {str(e)}")

        else:
            # Print total elapsed time after every successful action
            elapsed = (datetime.now() - start_time).total_seconds()
            minutes, seconds = divmod(int(elapsed), 60)
            hours, minutes = divmod(minutes, 60)
            if hours:
                duration = f"{hours}h {minutes}m {seconds}s"
            elif minutes:
                duration = f"{minutes}m {seconds}s"
            else:
                duration = f"{elapsed:.1f}s"
            self.stdout.write(self.style.HTTP_INFO(f"\nTotal time: {duration}"))

        finally:
            if log_file:
                end_time = datetime.now()
                log_file.write(f"\nEnded: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_file.close()
                tee._log_file = None  # prevent writes to closed file
                self.stdout.write(self.style.SUCCESS(f"\nLogs saved to {log_path}"))

    def handle_discover(self, options):
        """Discover and display schema structure."""
        schema_name = self._get_required_option(options, "schema")
        output_format = options["format"]

        self.stdout.write(self.style.WARNING(f"Discovering schema: {schema_name}"))

        with SnowflakeConnection() as sf_conn:
            discovery = SnowflakeSchemaDiscovery(sf_conn)
            schema = discovery.discover_schema(schema_name)

        if output_format == "json":
            self._output_schema_json(schema)
        else:
            self._output_schema_text(schema)

    def handle_build(self, options):
        """Build schema structure without data."""
        source_schema = self._get_required_option(options, "schema")
        target_schema = options.get("target") or source_schema.lower()
        db_alias = options["db"]
        dry_run = options["dry_run"]
        output_file = options.get("output")

        self.stdout.write(
            self.style.WARNING(f"Building schema: {source_schema} -> {target_schema}")
        )

        # Discover schema
        table_filter = options.get("table")
        with SnowflakeConnection() as sf_conn:
            discovery = SnowflakeSchemaDiscovery(sf_conn)
            schema = discovery.discover_schema(
                source_schema,
                table_filter=table_filter,
                status_callback=self._create_status_callback(),
            )

        # Generate DDL
        self.stdout.write(f"  Generating DDL for {len(schema.tables)} table(s)...")
        generator = PostgresDDLGenerator()
        ddl_statements = generator.generate_schema_ddl(schema, target_schema)

        # Output to file if specified
        if output_file:
            self._write_ddl_to_file(ddl_statements, output_file)
            self.stdout.write(self.style.SUCCESS(f"DDL written to: {output_file}"))
            return

        # Execute DDL
        with PostgresConnection(db_alias) as pg_conn:
            executor = PostgresDDLExecutor(pg_conn, dry_run=dry_run)

            real_count = sum(1 for s in ddl_statements if s.strip())
            if real_count == 0:
                self.stdout.write(
                    self.style.WARNING(
                        "No DDL statements to execute. "
                        "Check that the schema and table name are correct."
                    )
                )
                return

            self.stdout.write(f"Executing {real_count} DDL statements...")

            result = executor.execute_ddl(
                ddl_statements,
                stop_on_error=not options["continue_on_error"],
                progress_callback=self._create_progress_callback(),
            )

            self._display_execution_result(result)

    def handle_build_views(self, options):
        """Build views only."""
        source_schema = self._get_required_option(options, "schema")
        target_schema = options.get("target") or source_schema.lower()
        db_alias = options["db"]
        dry_run = options["dry_run"]
        output_file = options.get("output")

        self.stdout.write(
            self.style.WARNING(f"Building views: {source_schema} -> {target_schema}")
        )

        # Discover schema (views + procedures only; tables not needed here)
        with SnowflakeConnection() as sf_conn:
            discovery = SnowflakeSchemaDiscovery(sf_conn)
            schema = discovery.discover_schema(
                source_schema,
                status_callback=self._create_status_callback(),
            )

        if not schema.views and not schema.procedures:
            self.stdout.write("  No views or procedures found.")
            return

        self.stdout.write(
            f"  Found {len(schema.views)} view(s), {len(schema.procedures)} procedure(s)"
        )

        # Translate views
        view_translator = SnowflakeViewTranslator()
        translated_views = []  # list of (name, ddl, cross_schema_deps)
        failed_translation = []

        for view in schema.views:
            pg_ddl, reason, cross_deps = view_translator.translate_view(
                view.name, view.ddl or "", target_schema
            )
            if pg_ddl:
                translated_views.append((view.name, pg_ddl, cross_deps))
            else:
                failed_translation.append((view.name, reason or "unknown reason"))

        # Procedures always need manual work — collect as comments only
        proc_translator = SnowflakeProcedureTranslator()
        proc_ddl_statements = []
        for proc in schema.procedures:
            if proc.ddl:
                pg_ddl = proc_translator.translate_procedure(
                    proc.name, proc.ddl, target_schema
                )
                if pg_ddl:
                    proc_ddl_statements.append(pg_ddl)

        # Output to file if specified
        if output_file:
            all_statements = [ddl for _, ddl in translated_views] + proc_ddl_statements
            self._write_ddl_to_file(all_statements, output_file)
            self.stdout.write(
                self.style.SUCCESS(f"View/Procedure DDL written to: {output_file}")
            )
            return

        # Execute translated views against PostgreSQL (best effort)
        succeeded = []
        failed_execution = []  # list of (name, error, ddl)

        if translated_views:
            self.stdout.write(
                f"  Executing {len(translated_views)} translated view(s)..."
            )
            with PostgresConnection(db_alias) as pg_conn:
                for view_name, pg_ddl, cross_deps in translated_views:
                    if cross_deps:
                        self.stdout.write(
                            self.style.WARNING(
                                f"  ⚠ {view_name}: depends on unmigranted schema(s): "
                                + ", ".join(cross_deps)
                            )
                        )
                    if dry_run:
                        self.stdout.write(
                            self.style.HTTP_INFO(f"  [DRY RUN] {view_name}")
                        )
                        succeeded.append(view_name)
                        continue
                    try:
                        with pg_conn.connection() as conn:
                            conn.autocommit = True
                            with conn.cursor() as cur:
                                cur.execute(pg_ddl)
                        self.stdout.write(self.style.SUCCESS(f"  ✓ {view_name}"))
                        succeeded.append(view_name)
                    except Exception as e:
                        err_msg = str(e).split("\n")[0]  # first line only
                        self.stdout.write(
                            self.style.ERROR(f"  ✗ {view_name}: {err_msg}")
                        )
                        if cross_deps:
                            self.stdout.write(
                                f"      Migrate these schemas first: "
                                + ", ".join(
                                    sorted({d.split(".")[0] for d in cross_deps})
                                )
                            )
                        failed_execution.append((view_name, str(e), pg_ddl, cross_deps))

        # Summary
        self.stdout.write(
            f"\nViews: {len(succeeded)} succeeded, "
            f"{len(failed_execution)} failed, "
            f"{len(failed_translation)} could not be translated"
        )

        if failed_translation:
            self.stdout.write(self.style.WARNING("  Could not translate:"))
            for name, hint in failed_translation:
                self.stdout.write(f"    {name}")
                self.stdout.write(self.style.ERROR(f"      DDL starts: {hint}"))

        if failed_execution:
            self.stdout.write(self.style.WARNING("\nFailed views:"))
            for name, error, ddl, cross_deps in failed_execution:
                self.stdout.write(
                    self.style.ERROR(f"  {name}: {error.split(chr(10))[0]}")
                )
                if cross_deps:
                    self.stdout.write(
                        f"    Requires: {', '.join(cross_deps)} "
                        f"(migrate those schemas first)"
                    )

            saved_path = self._save_failed_views_ddl(options, failed_execution)
            if saved_path:
                self.stdout.write(
                    self.style.SUCCESS(f"\nFailed view DDL saved to: {saved_path}")
                )

        if proc_ddl_statements:
            self.stdout.write(
                self.style.WARNING(
                    f"\n{len(proc_ddl_statements)} procedure(s) require manual translation "
                    f"— use --output to save stubs."
                )
            )

    def handle_destroy(self, options):
        """Destroy schema and all its tables."""
        target_schema = self._get_required_option(options, "target")
        db_alias = options["db"]
        force = options["force"]
        dry_run = options["dry_run"]

        # Confirm destruction
        if not force and not dry_run:
            self.stdout.write(
                self.style.ERROR(
                    f"\nWARNING: This will PERMANENTLY DELETE schema '{target_schema}' and ALL its data!"
                )
            )
            confirm = input("Type the schema name to confirm: ")
            if confirm != target_schema:
                self.stdout.write(self.style.ERROR("Aborted."))
                return

        self.stdout.write(self.style.WARNING(f"Destroying schema: {target_schema}"))

        # Generate drop statements
        generator = PostgresDDLGenerator()
        drop_statements = generator.generate_drop_schema_ddl(target_schema)

        # Execute
        with PostgresConnection(db_alias) as pg_conn:
            executor = PostgresDDLExecutor(pg_conn, dry_run=dry_run)
            result = executor.execute_ddl(drop_statements)

            self._display_execution_result(result)

    def handle_migrate(self, options):
        """Full migration: build schema + transfer data + build views."""
        source_schema = self._get_required_option(options, "schema")
        target_schema = options.get("target") or source_schema.lower()

        self.stdout.write(self.style.WARNING("=== FULL MIGRATION ==="))
        self.stdout.write(f"Source: {source_schema}")
        self.stdout.write(f"Target: {target_schema}")

        # Step 1: Build schema
        self.stdout.write(self.style.WARNING("\n[1/3] Building schema structure..."))
        self.handle_build(options)

        # Step 2: Transfer data
        self.stdout.write(self.style.WARNING("\n[2/3] Transferring data..."))
        self.handle_transfer(options)

        # Step 3: Build views (best effort)
        self.stdout.write(self.style.WARNING("\n[3/3] Building views (best effort)..."))
        self.handle_build_views(options)

        self.stdout.write(self.style.SUCCESS("\n=== MIGRATION COMPLETE ==="))

    def handle_transfer(self, options):
        """Transfer data from Snowflake to PostgreSQL."""
        source_schema = self._get_required_option(options, "schema")
        target_schema = options.get("target") or source_schema.lower()
        db_alias = options["db"]
        batch_size = options["batch_size"]
        table_filter = [options["table"]] if options.get("table") else None
        where_clause = options.get("where")
        limit = options.get("limit")

        self.stdout.write(
            self.style.WARNING(f"Transferring data: {source_schema} -> {target_schema}")
        )
        if where_clause:
            self.stdout.write(f"  WHERE: {where_clause}")
        if limit:
            self.stdout.write(f"  LIMIT: {limit:,}")

        with SnowflakeConnection() as sf_conn, PostgresConnection(db_alias) as pg_conn:
            transfer_engine = DataTransferEngine(
                sf_conn, pg_conn, batch_size=batch_size
            )

            stats_list = transfer_engine.transfer_schema(
                source_schema=source_schema,
                target_schema=target_schema,
                table_filter=table_filter,
                where_clause=where_clause,
                limit=limit,
                progress_callback=self._create_transfer_progress_callback(),
                row_progress_callback=self._create_row_progress_callback(),
                status_callback=self._create_status_callback(),
            )

            self._display_transfer_stats(stats_list)

    def handle_validate(self, options):
        """Validate data integrity between Snowflake and PostgreSQL."""
        source_schema = self._get_required_option(options, "schema")
        target_schema = options.get("target") or source_schema.lower()
        db_alias = options["db"]
        table_name = options.get("table")
        sample_size = options.get("sample_size", 0)

        self.stdout.write(
            self.style.WARNING(f"Validating: {source_schema} -> {target_schema}")
        )
        if sample_size:
            self.stdout.write(f"  Row sample size: {sample_size:,} (Layer 5 enabled)")

        with SnowflakeConnection() as sf_conn, PostgresConnection(db_alias) as pg_conn:
            validator = DataValidator(
                sf_conn,
                pg_conn,
                sample_size=sample_size,
                status_callback=self._create_status_callback(),
            )

            if table_name:
                tables = [table_name.upper()]
            else:
                # Lightweight table list — no COUNT(*) like discover_schema does
                with sf_conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                        ORDER BY TABLE_NAME
                        """,
                        (source_schema,),
                    )
                    tables = [row["TABLE_NAME"] for row in cur.fetchall()]

            total = len(tables)
            results = []
            for i, table in enumerate(tables, 1):
                self.stdout.write(
                    self.style.HTTP_INFO(f"\n  [{i}/{total}] {table.lower()}")
                )
                result = validator.validate_table(
                    sf_schema=source_schema,
                    sf_table=table,
                    pg_schema=target_schema,
                    pg_table=table.lower(),
                )
                results.append(result)

            self._display_validation_results(results)

    def _display_validation_results(self, results):
        """Display validation results summary."""
        all_passed = all(r.passed for r in results)
        total_checks = sum(len(r.checks) for r in results)
        failed_checks = sum(len(r.failed_checks) for r in results)

        if all_passed:
            self.stdout.write(self.style.SUCCESS("\n=== Validation Complete ==="))
        else:
            self.stdout.write(self.style.ERROR("\n=== Validation FAILED ==="))

        for result in results:
            table_status = (
                self.style.SUCCESS("✓") if result.passed else self.style.ERROR("✗")
            )
            self.stdout.write(
                f"\n{table_status} {result.table_name}  ({result.duration:.1f}s)"
            )
            for check in result.checks:
                if check.passed is None:
                    icon = self.style.WARNING("  ⚠")
                elif check.passed:
                    icon = self.style.SUCCESS("  ✓")
                else:
                    icon = self.style.ERROR("  ✗")
                self.stdout.write(f"{icon} {check.name}: {check.message}")
                if check.details:
                    for detail in check.details[:10]:
                        self.stdout.write(f"      {detail}")
                    if len(check.details) > 10:
                        self.stdout.write(
                            f"      ... and {len(check.details) - 10} more"
                        )

        self.stdout.write(f"\nTables validated : {len(results)}")
        self.stdout.write(f"Checks run       : {total_checks}")
        self.stdout.write(f"Checks failed    : {failed_checks}")

        if all_passed:
            self.stdout.write(
                self.style.SUCCESS("\nAll checks passed. Data integrity confirmed.")
            )
        else:
            self.stdout.write(
                self.style.ERROR(
                    f"\n{failed_checks} check(s) failed. Review output above."
                )
            )

    def _get_required_option(self, options, key):
        """Get required option or raise error."""
        value = options.get(key)
        if not value:
            raise CommandError(f"--{key} is required for this action")
        return value

    def _output_schema_text(self, schema):
        """Output schema in human-readable text format."""
        self.stdout.write(self.style.SUCCESS(f"\n=== Schema: {schema.name} ==="))
        self.stdout.write(f"Database: {schema.database}")
        self.stdout.write(f"Tables: {len(schema.tables)}")
        self.stdout.write(f"Views: {len(schema.views)}")
        self.stdout.write(f"Procedures: {len(schema.procedures)}\n")

        for table in schema.tables:
            self.stdout.write(self.style.HTTP_INFO(f"\nTable: {table.name}"))
            self.stdout.write(f"  Rows: ~{table.row_count:,}")
            self.stdout.write(f"  Columns: {len(table.columns)}")

            if table.primary_key:
                pk_cols = ", ".join(table.primary_key.columns)
                self.stdout.write(f"  Primary Key: {pk_cols}")

            if table.foreign_keys:
                self.stdout.write(f"  Foreign Keys: {len(table.foreign_keys)}")

            # Show columns
            self.stdout.write("  Columns:")
            for col in table.columns[:10]:
                nullable = "NULL" if col.is_nullable else "NOT NULL"
                self.stdout.write(f"    - {col.name}: {col.data_type} {nullable}")

            if len(table.columns) > 10:
                self.stdout.write(f"    ... and {len(table.columns) - 10} more")

        # Show views
        if schema.views:
            self.stdout.write(self.style.HTTP_INFO("\nViews:"))
            for view in schema.views:
                self.stdout.write(f"  - {view.name}")

        # Show procedures
        if schema.procedures:
            self.stdout.write(self.style.HTTP_INFO("\nProcedures:"))
            for proc in schema.procedures:
                self.stdout.write(f"  - {proc.name}")

    def _output_schema_json(self, schema):
        """Output schema in JSON format."""
        output = {"schema": schema.name, "database": schema.database, "tables": []}

        for table in schema.tables:
            table_data = {
                "name": table.name,
                "row_count": table.row_count,
                "columns": [
                    {
                        "name": col.name,
                        "type": col.data_type,
                        "nullable": col.is_nullable,
                    }
                    for col in table.columns
                ],
            }
            output["tables"].append(table_data)

        self.stdout.write(json.dumps(output, indent=2))

    def _create_progress_callback(self):
        """Create progress callback for DDL execution."""

        def callback(current, total, statement):
            self.stdout.write(f"  [{current}/{total}] {statement[:80]}...")

        return callback

    def _create_transfer_progress_callback(self):
        """Create progress callback for data transfer."""

        def callback(table_name, current, total):
            self.stdout.write(f"  [{current}/{total}] Transferring: {table_name}")

        return callback

    def _create_row_progress_callback(self):
        """Create progress callback for per-batch row progress."""

        def callback(rows_so_far):
            self.stdout.write(f"    {rows_so_far:,} rows transferred so far...")
            self.stdout.flush()

        return callback

    def _create_status_callback(self):
        """Create callback for status messages during transfer."""

        def callback(message):
            self.stdout.write(f"    {message}")
            self.stdout.flush()

        return callback

    def _display_execution_result(self, result):
        """Display execution result summary."""
        if result.success:
            self.stdout.write(self.style.SUCCESS(f"\n✓ Success!"))
        else:
            self.stdout.write(self.style.ERROR(f"\n✗ Failed"))

        self.stdout.write(f"  Executed: {result.statements_executed}")
        self.stdout.write(f"  Failed: {result.statements_failed}")
        self.stdout.write(f"  Time: {result.execution_time:.2f}s")

        if result.warnings:
            self.stdout.write(self.style.WARNING("\nWarnings:"))
            for warning in result.warnings:
                self.stdout.write(f"  - {warning}")

        if result.errors:
            self.stdout.write(self.style.ERROR("\nErrors:"))
            for error in result.errors[:5]:
                self.stdout.write(f"  - {error}")

    def _display_transfer_stats(self, stats_list):
        """Display data transfer statistics."""
        total_rows = sum(s.rows_transferred for s in stats_list)
        total_time = sum(s.transfer_time for s in stats_list)
        successful = sum(1 for s in stats_list if s.success)

        self.stdout.write(self.style.SUCCESS(f"\n=== Transfer Complete ==="))
        self.stdout.write(f"Tables processed: {len(stats_list)}")
        self.stdout.write(f"Successful: {successful}")
        self.stdout.write(f"Failed: {len(stats_list) - successful}")
        self.stdout.write(f"Total rows: {total_rows:,}")
        self.stdout.write(f"Total time: {total_time:.2f}s")

        if total_time > 0:
            self.stdout.write(f"Average speed: {total_rows / total_time:.0f} rows/s")

        # Show per-table stats
        self.stdout.write("\nPer-table statistics:")
        for stats in stats_list:
            status = "✓" if stats.success else "✗"
            self.stdout.write(
                f"  {status} {stats.table_name}: "
                f"{stats.rows_transferred:,} rows in {stats.transfer_time:.2f}s "
                f"({stats.rows_per_second:.0f} rows/s)"
            )

            if not stats.success:
                self.stdout.write(
                    self.style.ERROR(f"      Error: {stats.error_message}")
                )

    def _assert_schema_exists(self, schema_name: str) -> None:
        """Raise CommandError immediately if the schema doesn't exist in Snowflake."""
        with SnowflakeConnection() as sf_conn:
            with sf_conn.cursor() as cur:
                cur.execute(
                    "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = %s",
                    (schema_name.upper(),),
                )
                if cur.fetchone():
                    return  # exists — all good

                # Not found — list available schemas to help the user
                try:
                    cur.execute(
                        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                        "ORDER BY SCHEMA_NAME"
                    )
                    available = [row["SCHEMA_NAME"] for row in cur.fetchall()]
                    hint = (
                        "Available schemas: " + ", ".join(available)
                        if available
                        else ""
                    )
                except Exception:
                    hint = "Could not list available schemas."

                raise CommandError(
                    f"Schema '{schema_name}' not found in Snowflake.\n{hint}"
                )

    def _post_action(self, options, action, verify=False):
        """Prompt user to verify migration after a command completes."""
        suppress = (
            options.get("force") or options.get("no_prompt") or options.get("dry_run")
        )

        if verify and not suppress:
            try:
                answer = input("\nVerify migration? [y/N]: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = ""
            if answer == "y":
                self.handle_validate(options)

    def _open_log_file(self, options, action, start_time):
        """Open a log file for streaming output and write the header immediately."""
        schema = options.get("schema") or options.get("target") or "unknown"
        target_schema = options.get("target") or schema.lower()
        timestamp = start_time.strftime("%Y%m%d_%H%M%S")

        folder = Path(f"logs/{timestamp}_{schema}")
        folder.mkdir(parents=True, exist_ok=True)
        log_path = folder / f"{action}.log"

        try:
            f = open(log_path, "w", encoding="utf-8")
            command_str = " ".join(sys.argv)
            f.write(f"=== sf_migrate {action} ===\n")
            f.write(f"Started:  {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Schema:   {schema}  ->  {target_schema}\n")
            f.write(f"Command:  {command_str}\n")
            f.write(f"\n--- OUTPUT ---\n")
            f.flush()
            return log_path, f
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Could not open log file: {e}"))
            return None, None

    def _save_failed_views_ddl(self, options, failed_execution):
        """Write DDL of failed views into the current log folder as failed_views.sql."""
        if self._log_folder:
            folder = self._log_folder
        else:
            # No active log session — create a standalone folder
            schema = options.get("schema") or options.get("target") or "unknown"
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            folder = Path(f"logs/{timestamp}_{schema}")
            folder.mkdir(parents=True, exist_ok=True)

        sql_path = folder / "failed_views.sql"
        try:
            with open(sql_path, "w", encoding="utf-8") as f:
                f.write(f"-- {len(failed_execution)} failed view(s) — fix and apply manually\n\n")
                for name, error, ddl, cross_deps in failed_execution:
                    f.write(f"-- View: {name}\n")
                    f.write(f"-- Error: {error.splitlines()[0]}\n")
                    if cross_deps:
                        f.write(f"-- Requires schemas: {', '.join(cross_deps)}\n")
                    f.write(f"{ddl.rstrip()}\n\n")
            return sql_path
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Could not save failed views DDL: {e}"))
            return None

    def _write_ddl_to_file(self, statements, filepath):
        """Write DDL statements to file."""
        with open(filepath, "w") as f:
            for stmt in statements:
                f.write(stmt)
                f.write("\n\n")
