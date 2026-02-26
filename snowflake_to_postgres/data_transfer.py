"""
snowflake_to_postgres/data_transfer.py

High-performance bulk data transfer from Snowflake to PostgreSQL.
"""

import csv
import io
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class TransferStats:
    """Statistics for data transfer operation."""

    table_name: str
    rows_transferred: int
    transfer_time: float
    rows_per_second: float
    success: bool
    error_message: Optional[str] = None


class DataTransferEngine:
    """Transfers data from Snowflake to PostgreSQL efficiently."""

    def __init__(
        self,
        sf_connection,
        pg_connection,
        batch_size: int = 10000,
        use_copy: bool = True,
    ):
        self.sf_conn = sf_connection
        self.pg_conn = pg_connection
        self.batch_size = batch_size
        self.use_copy = use_copy

    def _get_row_count_estimate(self, schema: str, table: str) -> Optional[int]:
        """Get approximate row count for a table."""
        query = f'SELECT COUNT(*) as "CNT" FROM {schema}.{table}'
        try:
            with self.sf_conn.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchone()
                return result["CNT"] if result else None
        except Exception:
            return None

    def transfer_table(
        self,
        source_schema: str,
        source_table: str,
        target_schema: str,
        target_table: Optional[str] = None,
        where_clause: Optional[str] = None,
        limit: Optional[int] = None,
        progress_callback: Optional[Callable[[int], None]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> TransferStats:
        """
        Transfer data from a Snowflake table to PostgreSQL.
        """
        target_table = target_table or source_table
        start_time = datetime.now()

        try:
            # Get column list
            columns = self._get_columns(source_schema, source_table)
            column_list = ", ".join([f'"{col}"' for col in columns])

            # Build SELECT query
            query = f"SELECT {column_list} FROM {source_schema}.{source_table}"
            if where_clause:
                query += f" WHERE {where_clause}"
            if limit:
                query += f" LIMIT {limit}"

            logger.info(
                f"Starting transfer: {source_schema}.{source_table} -> {target_schema}.{target_table}"
            )

            # Show row count estimate
            if status_callback:
                status_callback(f"Counting rows in {source_schema}.{source_table}...")
            row_estimate = self._get_row_count_estimate(source_schema, source_table)
            if status_callback and row_estimate is not None:
                status_callback(f"~{row_estimate:,} rows to transfer")

            # Transfer data
            if self.use_copy:
                rows_transferred = self._transfer_using_copy(
                    query, columns, target_schema, target_table, progress_callback, status_callback
                )
            else:
                rows_transferred = self._transfer_using_insert(
                    query, columns, target_schema, target_table, progress_callback, status_callback
                )

            end_time = datetime.now()
            transfer_time = (end_time - start_time).total_seconds()
            rows_per_second = (
                rows_transferred / transfer_time if transfer_time > 0 else 0
            )

            logger.info(
                f"Transfer complete: {rows_transferred} rows in {transfer_time:.2f}s ({rows_per_second:.0f} rows/s)"
            )

            return TransferStats(
                table_name=f"{source_schema}.{source_table}",
                rows_transferred=rows_transferred,
                transfer_time=transfer_time,
                rows_per_second=rows_per_second,
                success=True,
            )

        except Exception as e:
            end_time = datetime.now()
            transfer_time = (end_time - start_time).total_seconds()
            error_msg = f"Transfer failed: {str(e)}"
            logger.error(error_msg)

            return TransferStats(
                table_name=f"{source_schema}.{source_table}",
                rows_transferred=0,
                transfer_time=transfer_time,
                rows_per_second=0,
                success=False,
                error_message=error_msg,
            )

    def _transfer_using_copy(
        self,
        query: str,
        columns: List[str],
        target_schema: str,
        target_table: str,
        progress_callback: Optional[Callable[[int], None]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> int:
        """Transfer using PostgreSQL COPY protocol for maximum speed."""
        total_rows = 0

        # Get Snowflake cursor
        sf_conn = self.sf_conn.connect()
        sf_cursor = sf_conn.cursor()

        try:
            if status_callback:
                status_callback("Querying Snowflake...")

            # Run execute with a timer so the user sees elapsed time
            stop_ticker = threading.Event()

            def _tick():
                start = time.time()
                while not stop_ticker.wait(10):
                    elapsed = int(time.time() - start)
                    if status_callback:
                        status_callback(f"Still waiting for Snowflake... ({elapsed}s elapsed)")

            ticker = threading.Thread(target=_tick, daemon=True)
            ticker.start()
            try:
                sf_cursor.execute(query)
            finally:
                stop_ticker.set()
                ticker.join()

            if status_callback:
                status_callback("Fetching and inserting rows...")

            with self.pg_conn.connection() as pg_conn:
                pg_cursor = pg_conn.cursor()

                # Create CSV buffer
                buffer = io.StringIO()
                writer = csv.writer(buffer)

                batch_count = 0
                while True:
                    rows = sf_cursor.fetchmany(self.batch_size)
                    if not rows:
                        break

                    # Write rows to CSV buffer
                    buffer.seek(0)
                    buffer.truncate()

                    for row in rows:
                        # Convert None to empty string, handle types
                        clean_row = ["" if val is None else str(val) for val in row]
                        writer.writerow(clean_row)

                    # Use COPY to load data
                    buffer.seek(0)
                    column_list = ", ".join([f'"{col.lower()}"' for col in columns])
                    copy_sql = f'COPY {target_schema}."{target_table}" ({column_list}) FROM STDIN WITH CSV'

                    pg_cursor.copy_expert(copy_sql, buffer)
                    pg_conn.commit()

                    batch_count += 1
                    total_rows += len(rows)

                    if progress_callback:
                        progress_callback(total_rows)

                    logger.debug(
                        f"Batch {batch_count}: {len(rows)} rows transferred (total: {total_rows})"
                    )

                pg_cursor.close()

        finally:
            sf_cursor.close()

        return total_rows

    def _transfer_using_insert(
        self,
        query: str,
        columns: List[str],
        target_schema: str,
        target_table: str,
        progress_callback: Optional[Callable[[int], None]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> int:
        """Transfer using batch INSERT statements."""
        total_rows = 0

        # Get Snowflake cursor
        sf_conn = self.sf_conn.connect()
        sf_cursor = sf_conn.cursor()

        try:
            if status_callback:
                status_callback("Querying Snowflake...")

            stop_ticker = threading.Event()

            def _tick():
                start = time.time()
                while not stop_ticker.wait(10):
                    elapsed = int(time.time() - start)
                    if status_callback:
                        status_callback(f"Still waiting for Snowflake... ({elapsed}s elapsed)")

            ticker = threading.Thread(target=_tick, daemon=True)
            ticker.start()
            try:
                sf_cursor.execute(query)
            finally:
                stop_ticker.set()
                ticker.join()

            if status_callback:
                status_callback("Fetching and inserting rows...")

            with self.pg_conn.connection() as pg_conn:
                pg_cursor = pg_conn.cursor()

                batch_count = 0
                while True:
                    rows = sf_cursor.fetchmany(self.batch_size)
                    if not rows:
                        break

                    # Build batch INSERT
                    column_list = ", ".join([f'"{col.lower()}"' for col in columns])
                    placeholders = ", ".join(["%s"] * len(columns))
                    insert_sql = f'INSERT INTO {target_schema}."{target_table}" ({column_list}) VALUES ({placeholders})'

                    # Execute batch
                    pg_cursor.executemany(insert_sql, rows)
                    pg_conn.commit()

                    batch_count += 1
                    total_rows += len(rows)

                    if progress_callback:
                        progress_callback(total_rows)

                    logger.debug(
                        f"Batch {batch_count}: {len(rows)} rows inserted (total: {total_rows})"
                    )

                pg_cursor.close()

        finally:
            sf_cursor.close()

        return total_rows

    def _get_columns(self, schema: str, table: str) -> List[str]:
        """Get column names for a table. Returns names in their original Snowflake case."""
        query = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = UPPER(%s)
        ORDER BY ORDINAL_POSITION
        """

        with self.sf_conn.cursor() as cursor:
            cursor.execute(query, (schema, table))
            return [row["COLUMN_NAME"] for row in cursor.fetchall()]

    def transfer_schema(
        self,
        source_schema: str,
        target_schema: str,
        table_filter: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        limit: Optional[int] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
        row_progress_callback: Optional[Callable[[int], None]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> List[TransferStats]:
        """
        Transfer all tables in a schema.
        """
        # Get list of tables
        tables = self._get_tables(source_schema)

        if table_filter:
            table_filter_lower = [t.lower() for t in table_filter]
            tables = [t for t in tables if t in table_filter_lower]

        stats_list = []
        total_tables = len(tables)

        for i, table in enumerate(tables, 1):
            if progress_callback:
                progress_callback(table, i, total_tables)

            stats = self.transfer_table(
                source_schema=source_schema,
                source_table=table,
                target_schema=target_schema,
                where_clause=where_clause,
                limit=limit,
                progress_callback=row_progress_callback,
                status_callback=status_callback,
            )
            stats_list.append(stats)

        return stats_list

    def _get_tables(self, schema: str) -> List[str]:
        """Get all table names in schema."""
        query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """

        with self.sf_conn.cursor() as cursor:
            cursor.execute(query, (schema,))
            return [row["TABLE_NAME"].lower() for row in cursor.fetchall()]
