"""
snowflake_to_postgres/data_transfer.py

High-performance bulk data transfer from Snowflake to PostgreSQL.
"""

import csv
import io
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

# Snowflake error codes that indicate a session/auth expiry
_SF_AUTH_ERROR_CODES = {"390114", "000403"}


def _is_sf_auth_error(exc: Exception) -> bool:
    """Return True if the exception is a Snowflake auth/session expiry error."""
    msg = str(exc)
    return any(code in msg for code in _SF_AUTH_ERROR_CODES)


def _build_resume_query(original_query: str, offset: int) -> str:
    """
    Append OFFSET (and adjust LIMIT if present) to resume a query from *offset* rows in.

    NOTE: Snowflake does not guarantee row order without an explicit ORDER BY, so
    this works best when the table has a natural append-only order.  For migration
    purposes the slight non-determinism is acceptable.
    """
    import re as _re

    upper = original_query.upper()
    limit_match = _re.search(r"\bLIMIT\s+(\d+)", upper)
    if limit_match:
        original_limit = int(limit_match.group(1))
        new_limit = max(original_limit - offset, 0)
        query = _re.sub(
            r"\bLIMIT\s+\d+", f"LIMIT {new_limit}", original_query, flags=_re.IGNORECASE
        )
        return f"{query} OFFSET {offset}"
    # Snowflake requires LIMIT before OFFSET — use a max-int sentinel
    return f"{original_query} LIMIT 2147483647 OFFSET {offset}"


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
        start_offset: int = 0,
        progress_callback: Optional[Callable[[int], None]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
        checkpoint_callback: Optional[Callable[[int], None]] = None,
    ) -> TransferStats:
        """
        Transfer data from a Snowflake table to PostgreSQL.

        start_offset: number of rows already committed (checkpoint resume).
                      The query will be issued with OFFSET start_offset so
                      previously transferred rows are skipped.
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
            if start_offset:
                query = _build_resume_query(query, start_offset)
                if status_callback:
                    status_callback(f"Resuming from row {start_offset:,}")

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
                    query,
                    columns,
                    target_schema,
                    target_table,
                    progress_callback,
                    status_callback,
                    checkpoint_callback,
                    start_offset,
                )
            else:
                rows_transferred = self._transfer_using_insert(
                    query,
                    columns,
                    target_schema,
                    target_table,
                    progress_callback,
                    status_callback,
                    checkpoint_callback,
                    start_offset,
                )

            rows_transferred += start_offset  # include already-committed rows in total
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
        checkpoint_callback: Optional[Callable[[int], None]] = None,
        start_offset: int = 0,
    ) -> int:
        """Transfer using PostgreSQL COPY protocol for maximum speed."""
        total_rows = 0

        # Get Snowflake cursor
        sf_conn = self.sf_conn.connect()
        sf_cursor = sf_conn.cursor()

        try:
            if status_callback:
                status_callback("Querying Snowflake...")

            def _execute_with_ticker(cursor, exec_query):
                """Execute query on cursor, printing elapsed time every 10 s."""
                stop_ticker = threading.Event()

                def _tick():
                    start = time.time()
                    while not stop_ticker.wait(10):
                        elapsed = int(time.time() - start)
                        if status_callback:
                            status_callback(
                                f"Still waiting for Snowflake... ({elapsed}s elapsed)"
                            )

                ticker = threading.Thread(target=_tick, daemon=True)
                ticker.start()
                try:
                    cursor.execute(exec_query)
                finally:
                    stop_ticker.set()
                    ticker.join()

            try:
                _execute_with_ticker(sf_cursor, query)
            except Exception as exec_err:
                if _is_sf_auth_error(exec_err):
                    if status_callback:
                        status_callback(
                            "Snowflake session expired before query — reconnecting..."
                        )
                    logger.warning(
                        f"Snowflake auth error on execute, reconnecting: {exec_err}"
                    )
                    sf_cursor.close()
                    sf_conn = self.sf_conn.reconnect()
                    sf_cursor = sf_conn.cursor()
                    _execute_with_ticker(sf_cursor, query)
                else:
                    raise

            if status_callback:
                status_callback("Fetching and inserting rows...")

            with self.pg_conn.connection() as pg_conn:
                pg_cursor = pg_conn.cursor()

                # Create CSV buffer.
                # Use \N as the explicit NULL marker (PG standard) with QUOTE_MINIMAL.
                # \N contains no CSV special chars so csv.writer leaves it unquoted;
                # PG COPY NULL '\N' matches only unquoted \N → DB NULL.
                # Empty strings from Snowflake become unquoted empty fields which,
                # because NULL is now '\N' (not empty), PG stores as '' not NULL.
                _NULL_MARKER = "\\N"
                buffer = io.StringIO()
                writer = csv.writer(buffer)

                batch_count = 0
                column_list = ", ".join([f'"{col.lower()}"' for col in columns])
                copy_sql = (
                    f'COPY {target_schema}."{target_table}" ({column_list}) '
                    f"FROM STDIN WITH CSV NULL '{_NULL_MARKER}'"
                )

                while True:
                    batch_count += 1
                    if status_callback:
                        status_callback(
                            f"Batch {batch_count}: fetching up to {self.batch_size:,} rows from Snowflake..."
                        )
                    try:
                        rows = sf_cursor.fetchmany(self.batch_size)
                    except Exception as fetch_err:
                        if _is_sf_auth_error(fetch_err) and total_rows > 0:
                            if status_callback:
                                status_callback(
                                    f"Snowflake session expired after {total_rows:,} rows — reconnecting and resuming..."
                                )
                            logger.warning(
                                f"Snowflake auth error at row {total_rows}, reconnecting: {fetch_err}"
                            )
                            sf_cursor.close()
                            sf_conn = self.sf_conn.reconnect()
                            sf_cursor = sf_conn.cursor()
                            resume_query = _build_resume_query(query, total_rows)
                            sf_cursor.execute(resume_query)
                            continue
                        raise

                    if not rows:
                        if status_callback:
                            status_callback("No more rows — transfer complete.")
                        break

                    if status_callback:
                        status_callback(
                            f"Batch {batch_count}: writing {len(rows):,} rows to PostgreSQL (COPY)..."
                        )

                    # Write rows to CSV buffer
                    buffer.seek(0)
                    buffer.truncate()

                    for row in rows:
                        # None  → \N  (unquoted by QUOTE_MINIMAL) → COPY NULL '\N' → DB NULL
                        # ''    → ''  (unquoted empty)             → PG stores as ''  (not NULL)
                        # other → str(val) as normal CSV field
                        clean_row = [
                            _NULL_MARKER if val is None else str(val) for val in row
                        ]
                        writer.writerow(clean_row)

                    # Use COPY to load data
                    buffer.seek(0)
                    pg_cursor.copy_expert(copy_sql, buffer)
                    pg_conn.commit()

                    total_rows += len(rows)

                    if checkpoint_callback:
                        checkpoint_callback(start_offset + total_rows)

                    if progress_callback:
                        progress_callback(total_rows)

                    if status_callback:
                        status_callback(
                            f"Batch {batch_count}: done — {total_rows:,} rows inserted so far."
                        )

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
        checkpoint_callback: Optional[Callable[[int], None]] = None,
        start_offset: int = 0,
    ) -> int:
        """Transfer using batch INSERT statements."""
        total_rows = 0

        # Get Snowflake cursor
        sf_conn = self.sf_conn.connect()
        sf_cursor = sf_conn.cursor()

        try:
            if status_callback:
                status_callback("Querying Snowflake...")

            def _execute_with_ticker(cursor, exec_query):
                """Execute query on cursor, printing elapsed time every 10 s."""
                stop_ticker = threading.Event()

                def _tick():
                    start = time.time()
                    while not stop_ticker.wait(10):
                        elapsed = int(time.time() - start)
                        if status_callback:
                            status_callback(
                                f"Still waiting for Snowflake... ({elapsed}s elapsed)"
                            )

                ticker = threading.Thread(target=_tick, daemon=True)
                ticker.start()
                try:
                    cursor.execute(exec_query)
                finally:
                    stop_ticker.set()
                    ticker.join()

            try:
                _execute_with_ticker(sf_cursor, query)
            except Exception as exec_err:
                if _is_sf_auth_error(exec_err):
                    if status_callback:
                        status_callback(
                            "Snowflake session expired before query — reconnecting..."
                        )
                    logger.warning(
                        f"Snowflake auth error on execute, reconnecting: {exec_err}"
                    )
                    sf_cursor.close()
                    sf_conn = self.sf_conn.reconnect()
                    sf_cursor = sf_conn.cursor()
                    _execute_with_ticker(sf_cursor, query)
                else:
                    raise

            if status_callback:
                status_callback("Fetching and inserting rows...")

            with self.pg_conn.connection() as pg_conn:
                pg_cursor = pg_conn.cursor()

                batch_count = 0
                column_list = ", ".join([f'"{col.lower()}"' for col in columns])
                placeholders = ", ".join(["%s"] * len(columns))
                insert_sql = f'INSERT INTO {target_schema}."{target_table}" ({column_list}) VALUES ({placeholders})'

                while True:
                    batch_count += 1
                    if status_callback:
                        status_callback(
                            f"Batch {batch_count}: fetching up to {self.batch_size:,} rows from Snowflake..."
                        )
                    try:
                        rows = sf_cursor.fetchmany(self.batch_size)
                    except Exception as fetch_err:
                        if _is_sf_auth_error(fetch_err) and total_rows > 0:
                            if status_callback:
                                status_callback(
                                    f"Snowflake session expired after {total_rows:,} rows — reconnecting and resuming..."
                                )
                            logger.warning(
                                f"Snowflake auth error at row {total_rows}, reconnecting: {fetch_err}"
                            )
                            sf_cursor.close()
                            sf_conn = self.sf_conn.reconnect()
                            sf_cursor = sf_conn.cursor()
                            resume_query = _build_resume_query(query, total_rows)
                            sf_cursor.execute(resume_query)
                            continue
                        raise

                    if not rows:
                        if status_callback:
                            status_callback("No more rows — transfer complete.")
                        break

                    if status_callback:
                        status_callback(
                            f"Batch {batch_count}: writing {len(rows):,} rows to PostgreSQL (INSERT)..."
                        )

                    # Execute batch
                    pg_cursor.executemany(insert_sql, rows)
                    pg_conn.commit()

                    total_rows += len(rows)

                    if checkpoint_callback:
                        checkpoint_callback(start_offset + total_rows)

                    if progress_callback:
                        progress_callback(total_rows)

                    if status_callback:
                        status_callback(
                            f"Batch {batch_count}: done — {total_rows:,} rows inserted so far."
                        )

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
        workers: int = 1,
        checkpoint=None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
        row_progress_callback: Optional[Callable[[int], None]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
    ) -> List[TransferStats]:
        """
        Transfer all tables in a schema.

        checkpoint: optional CheckpointManager.  Completed tables are skipped;
                    interrupted tables resume from their last committed row.
        workers:    number of parallel threads (each gets its own SF connection).
        """
        from .connections import SnowflakeConnection  # local import avoids circular dep

        # Get list of tables
        tables = self._get_tables(source_schema)

        if table_filter:
            table_filter_lower = [t.lower() for t in table_filter]
            tables = [t for t in tables if t in table_filter_lower]

        # Skip already-completed tables
        if checkpoint:
            skipped = [t for t in tables if checkpoint.is_completed(t)]
            if skipped and status_callback:
                status_callback(
                    f"Checkpoint: skipping {len(skipped)} already-completed table(s): "
                    + ", ".join(skipped)
                )
            tables = [t for t in tables if not checkpoint.is_completed(t)]

        total_tables = len(tables)

        def _make_checkpoint_cb(table):
            if checkpoint:
                return lambda rows: checkpoint.update_progress(table, rows)
            return None

        def _on_table_done(table, stats):
            if checkpoint and stats.success:
                checkpoint.mark_completed(table)

        if workers <= 1:
            stats_list = []
            for i, table in enumerate(tables, 1):
                if progress_callback:
                    progress_callback(table, i, total_tables)
                start_offset = checkpoint.get_offset(table) if checkpoint else 0
                if start_offset and status_callback:
                    status_callback(
                        f"Resuming {table} from row {start_offset:,} (checkpoint)"
                    )
                stats = self.transfer_table(
                    source_schema=source_schema,
                    source_table=table,
                    target_schema=target_schema,
                    where_clause=where_clause,
                    limit=limit,
                    start_offset=start_offset,
                    progress_callback=row_progress_callback,
                    status_callback=status_callback,
                    checkpoint_callback=_make_checkpoint_cb(table),
                )
                _on_table_done(table, stats)
                stats_list.append(stats)
            return stats_list

        # --- Parallel path ---
        sf_config = self.sf_conn.config
        _print_lock = threading.Lock()

        def _prefixed_status(table_name, msg):
            if status_callback:
                with _print_lock:
                    status_callback(f"[{table_name}] {msg}")

        def _prefixed_row_progress(table_name, rows_so_far):
            if row_progress_callback:
                with _print_lock:
                    row_progress_callback(rows_so_far)

        completed_count = [0]

        def _transfer_one(table):
            start_offset = checkpoint.get_offset(table) if checkpoint else 0
            if start_offset:
                _prefixed_status(
                    table, f"Resuming from row {start_offset:,} (checkpoint)"
                )
            sf_conn = SnowflakeConnection(sf_config)
            engine = DataTransferEngine(
                sf_conn, self.pg_conn, self.batch_size, self.use_copy
            )
            try:
                stats = engine.transfer_table(
                    source_schema=source_schema,
                    source_table=table,
                    target_schema=target_schema,
                    where_clause=where_clause,
                    limit=limit,
                    start_offset=start_offset,
                    progress_callback=lambda n: _prefixed_row_progress(table, n),
                    status_callback=lambda m: _prefixed_status(table, m),
                    checkpoint_callback=_make_checkpoint_cb(table),
                )
            finally:
                sf_conn.close()

            _on_table_done(table, stats)

            with _print_lock:
                completed_count[0] += 1
                if progress_callback:
                    progress_callback(table, completed_count[0], total_tables)

            return stats

        stats_list = [None] * total_tables
        table_index = {t: i for i, t in enumerate(tables)}

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_table = {executor.submit(_transfer_one, t): t for t in tables}
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                stats_list[table_index[table]] = future.result()

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
