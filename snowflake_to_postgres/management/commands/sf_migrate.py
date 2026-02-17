"""
snowflake_to_postgres/management/commands/sf_migrate.py

Django management command for Snowflake to PostgreSQL migration.
"""

import json

from django.core.management.base import BaseCommand, CommandError

from ...connections import PostgresConnection, SnowflakeConnection
from ...data_transfer import DataTransferEngine
from ...discovery import SnowflakeSchemaDiscovery
from ...executor import PostgresDDLExecutor
from ...translator import PostgresDDLGenerator
from ...view_procedure_translator import (
    SnowflakeProcedureTranslator,
    SnowflakeViewTranslator,
)


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
            help='SQL WHERE clause to filter rows (e.g., --where "DATE >= \'2025-01-01\'")',
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

    def handle(self, *args, **options):
        action = options["action"]

        try:
            if action == "discover":
                self.handle_discover(options)
            elif action == "build":
                self.handle_build(options)
            elif action == "build-views":
                self.handle_build_views(options)
            elif action == "destroy":
                self.handle_destroy(options)
            elif action == "migrate":
                self.handle_migrate(options)
            elif action == "transfer":
                self.handle_transfer(options)

        except Exception as e:
            raise CommandError(f"Error during {action}: {str(e)}")

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
            schema = discovery.discover_schema(source_schema, table_filter=table_filter)

        # Generate DDL
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

            self.stdout.write(f"Executing {len(ddl_statements)} DDL statements...")

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
        output_file = options.get("output")

        self.stdout.write(
            self.style.WARNING(f"Building views: {source_schema} -> {target_schema}")
        )

        # Discover schema
        with SnowflakeConnection() as sf_conn:
            discovery = SnowflakeSchemaDiscovery(sf_conn)
            schema = discovery.discover_schema(source_schema)

        # Translate views
        view_translator = SnowflakeViewTranslator()
        view_ddl_statements = []

        for view in schema.views:
            if view.ddl:
                pg_ddl = view_translator.translate_view(
                    view.name, view.ddl, target_schema
                )
                if pg_ddl:
                    view_ddl_statements.append(pg_ddl)
                    view_ddl_statements.append("")

        # Translate procedures
        proc_translator = SnowflakeProcedureTranslator()
        for proc in schema.procedures:
            if proc.ddl:
                pg_ddl = proc_translator.translate_procedure(
                    proc.name, proc.ddl, target_schema
                )
                if pg_ddl:
                    view_ddl_statements.append(pg_ddl)
                    view_ddl_statements.append("")

        # Output to file if specified
        if output_file:
            self._write_ddl_to_file(view_ddl_statements, output_file)
            self.stdout.write(
                self.style.SUCCESS(f"View/Procedure DDL written to: {output_file}")
            )
            return

        self.stdout.write(
            self.style.WARNING(
                "\nViews and procedures require manual review.\n"
                "Use --output to save DDL for editing before execution."
            )
        )

        for stmt in view_ddl_statements:
            self.stdout.write(stmt)

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
        """Full migration: build schema + transfer data."""
        source_schema = self._get_required_option(options, "schema")
        target_schema = options.get("target") or source_schema.lower()

        self.stdout.write(self.style.WARNING("=== FULL MIGRATION ==="))
        self.stdout.write(f"Source: {source_schema}")
        self.stdout.write(f"Target: {target_schema}")

        # Step 1: Build schema
        self.stdout.write(self.style.WARNING("\n[1/2] Building schema structure..."))
        self.handle_build(options)

        # Step 2: Transfer data
        self.stdout.write(self.style.WARNING("\n[2/2] Transferring data..."))
        self.handle_transfer(options)

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

    def _write_ddl_to_file(self, statements, filepath):
        """Write DDL statements to file."""
        with open(filepath, "w") as f:
            for stmt in statements:
                f.write(stmt)
                f.write("\n\n")
