"""
snowflake_to_postgres/executor.py

Executes DDL statements on PostgreSQL with transaction management.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Result of DDL execution."""

    success: bool
    statements_executed: int
    statements_failed: int
    execution_time: float
    errors: List[str]
    warnings: List[str]


class PostgresDDLExecutor:
    """Executes DDL statements on PostgreSQL database."""

    def __init__(self, pg_connection, dry_run: bool = False):
        self.pg_conn = pg_connection
        self.dry_run = dry_run

    def execute_ddl(
        self,
        statements: List[str],
        stop_on_error: bool = True,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> ExecutionResult:
        """
        Execute a list of DDL statements.

        Args:
            statements: List of DDL statements to execute
            stop_on_error: Stop execution on first error
            progress_callback: Optional callback for progress updates

        Returns:
            ExecutionResult with execution summary
        """
        start_time = datetime.now()
        executed = 0
        failed = 0
        errors = []
        warnings = []

        if self.dry_run:
            logger.info("DRY RUN MODE - No statements will be executed")
            statements = [s for s in statements if s.strip()]
            for i, stmt in enumerate(statements, 1):
                if progress_callback:
                    progress_callback(i, len(statements), stmt[:100])
                logger.info(f"[DRY RUN] Statement {i}/{len(statements)}:\n{stmt}")

            end_time = datetime.now()
            return ExecutionResult(
                success=True,
                statements_executed=len(statements),
                statements_failed=0,
                execution_time=(end_time - start_time).total_seconds(),
                errors=[],
                warnings=["DRY RUN MODE - No changes were made"],
            )

        # Execute statements
        with self.pg_conn.connection() as conn:
            conn.autocommit = False
            cursor = conn.cursor()

            # Filter empty statements so progress counts and totals are accurate
            statements = [s for s in statements if s.strip()]

            try:
                for i, statement in enumerate(statements, 1):
                    if progress_callback:
                        progress_callback(i, len(statements), statement[:100])

                    try:
                        logger.debug(f"Executing statement {i}/{len(statements)}")
                        cursor.execute(statement)
                        executed += 1

                    except Exception as e:
                        failed += 1
                        error_msg = f"Statement {i} failed: {str(e)}\nStatement: {statement[:200]}"
                        errors.append(error_msg)
                        logger.error(error_msg)

                        if stop_on_error:
                            conn.rollback()
                            raise

                # Commit all changes
                conn.commit()
                logger.info(f"Successfully executed {executed} statements")

            except Exception as e:
                conn.rollback()
                logger.error(f"Transaction rolled back due to error: {e}")
                raise

            finally:
                cursor.close()

        end_time = datetime.now()

        return ExecutionResult(
            success=failed == 0,
            statements_executed=executed,
            statements_failed=failed,
            execution_time=(end_time - start_time).total_seconds(),
            errors=errors,
            warnings=warnings,
        )
