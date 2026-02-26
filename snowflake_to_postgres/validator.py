"""
snowflake_to_postgres/validator.py

Data validation engine for comparing Snowflake and PostgreSQL data after transfer.
Runs up to 5 layers of checks, from fast row counts to row-level sampling.
"""

import logging
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Result of a single validation check."""

    name: str
    passed: Optional[bool]  # None = skipped / inconclusive
    source_value: Any
    target_value: Any
    message: str
    details: Optional[List[str]] = None


@dataclass
class TableValidationResult:
    """Aggregated validation results for one table."""

    table_name: str
    checks: List[CheckResult] = field(default_factory=list)
    duration: float = 0.0

    @property
    def passed(self) -> bool:
        return all(c.passed is not False for c in self.checks)

    @property
    def failed_checks(self) -> List[CheckResult]:
        return [c for c in self.checks if c.passed is False]


class DataValidator:
    """
    Validates data integrity between a Snowflake source and a PostgreSQL target.

    Layers:
      1 – Total row count
      2 – Per-partition row counts (grouped by date column, if present)
      3 – Column-level statistics (NULL counts + MIN/MAX), one scan each side
      4 – Aggregate fingerprint (SUM of numeric cols per date partition)
      5 – Row-level sample comparison (opt-in via sample_size > 0, requires PK)
    """

    # Max columns processed in a single SQL expression list
    _NULL_CHUNK = 50
    _MINMAX_CHUNK = 25

    def __init__(
        self,
        sf_connection,
        pg_connection,
        sample_size: int = 0,
        status_callback: Optional[Callable[[str], None]] = None,
    ):
        self.sf_conn = sf_connection
        self.pg_conn = pg_connection
        self.sample_size = sample_size
        self.status_callback = status_callback

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_table(
        self,
        sf_schema: str,
        sf_table: str,
        pg_schema: str,
        pg_table: Optional[str] = None,
    ) -> TableValidationResult:
        """Run all validation layers for a single table."""
        pg_table = pg_table or sf_table.lower()
        start = time.time()
        result = TableValidationResult(table_name=f"{sf_schema}.{sf_table}")

        columns = self._get_columns(sf_schema, sf_table)
        if not columns:
            result.checks.append(
                CheckResult(
                    name="schema_check",
                    passed=False,
                    source_value=None,
                    target_value=None,
                    message=f"No columns found for {sf_schema}.{sf_table} — table may not exist",
                )
            )
            result.duration = time.time() - start
            return result

        date_col = self._detect_date_column(columns)
        numeric_cols = self._get_numeric_columns(columns)

        # Layer 1: row count
        self._status("  Layer 1: Row count...")
        result.checks.append(
            self._check_row_count(sf_schema, sf_table, pg_schema, pg_table)
        )

        # Layer 2: per-partition counts
        if date_col:
            self._status(f"  Layer 2: Partition counts by '{date_col}'...")
            result.checks.append(
                self._check_partition_counts(
                    sf_schema, sf_table, pg_schema, pg_table, date_col
                )
            )
        else:
            self._status("  Layer 2: Skipped (no date column detected)")

        # Layer 3: column-level stats
        self._status("  Layer 3: Column statistics...")
        result.checks.extend(
            self._check_column_stats(
                sf_schema, sf_table, pg_schema, pg_table, columns
            )
        )

        # Layer 4: aggregate fingerprint
        if numeric_cols and date_col:
            self._status("  Layer 4: Aggregate fingerprint...")
            result.checks.append(
                self._check_aggregate_fingerprint(
                    sf_schema, sf_table, pg_schema, pg_table, date_col, numeric_cols
                )
            )
        else:
            self._status("  Layer 4: Skipped (need both numeric and date columns)")

        # Layer 5: row sample (opt-in)
        if self.sample_size > 0:
            self._status(f"  Layer 5: Row sample ({self.sample_size:,} rows)...")
            result.checks.append(
                self._check_row_sample(
                    sf_schema, sf_table, pg_schema, pg_table, columns
                )
            )

        result.duration = time.time() - start
        return result

    # ------------------------------------------------------------------
    # Layer implementations
    # ------------------------------------------------------------------

    def _check_row_count(
        self, sf_schema, sf_table, pg_schema, pg_table
    ) -> CheckResult:
        sf_count = self._sf_scalar(
            f"SELECT COUNT(*) as CNT FROM {sf_schema}.{sf_table}", "CNT"
        )
        pg_count = self._pg_scalar(
            f'SELECT COUNT(*) as cnt FROM {pg_schema}."{pg_table}"', "cnt"
        )
        passed = sf_count == pg_count
        delta = abs(sf_count - pg_count) if None not in (sf_count, pg_count) else None
        return CheckResult(
            name="row_count",
            passed=passed,
            source_value=sf_count,
            target_value=pg_count,
            message=(
                f"Row counts match ({sf_count:,})"
                if passed
                else f"Mismatch: SF={sf_count:,}  PG={pg_count:,}  delta={delta:,}"
            ),
        )

    def _check_partition_counts(
        self, sf_schema, sf_table, pg_schema, pg_table, date_col
    ) -> CheckResult:
        sf_counts = self._sf_group_count(sf_schema, sf_table, date_col)
        pg_counts = self._pg_group_count(pg_schema, pg_table, date_col.lower())

        all_dates = set(sf_counts) | set(pg_counts)
        mismatches = []
        for d in sorted(all_dates):
            sf_v = sf_counts.get(d, 0)
            pg_v = pg_counts.get(d, 0)
            if sf_v != pg_v:
                mismatches.append(
                    f"  {d}: SF={sf_v:,}  PG={pg_v:,}  delta={abs(sf_v - pg_v):,}"
                )

        passed = len(mismatches) == 0
        return CheckResult(
            name="partition_counts",
            passed=passed,
            source_value=len(sf_counts),
            target_value=len(pg_counts),
            message=(
                f"Partition counts match ({len(all_dates)} partitions)"
                if passed
                else f"{len(mismatches)}/{len(all_dates)} partitions mismatched"
            ),
            details=mismatches[:25],
        )

    def _check_column_stats(
        self, sf_schema, sf_table, pg_schema, pg_table, columns
    ) -> List[CheckResult]:
        results = []

        # NULL counts — single table scan per side
        null_mismatches = self._compare_null_counts(
            sf_schema, sf_table, pg_schema, pg_table, columns
        )
        results.append(
            CheckResult(
                name="null_counts",
                passed=len(null_mismatches) == 0,
                source_value=None,
                target_value=None,
                message=(
                    f"NULL counts match across all {len(columns)} columns"
                    if not null_mismatches
                    else f"{len(null_mismatches)} column(s) have NULL count mismatches"
                ),
                details=null_mismatches,
            )
        )

        # MIN / MAX — single scan for numeric + date columns
        checkable = [c for c in columns if self._is_numeric_or_date(c["type"])]
        if checkable:
            minmax_mismatches = self._compare_minmax(
                sf_schema, sf_table, pg_schema, pg_table, checkable
            )
            results.append(
                CheckResult(
                    name="min_max_values",
                    passed=len(minmax_mismatches) == 0,
                    source_value=None,
                    target_value=None,
                    message=(
                        f"MIN/MAX values match ({len(checkable)} columns checked)"
                        if not minmax_mismatches
                        else f"{len(minmax_mismatches)} column(s) have MIN/MAX mismatches"
                    ),
                    details=minmax_mismatches,
                )
            )

        return results

    def _check_aggregate_fingerprint(
        self, sf_schema, sf_table, pg_schema, pg_table, date_col, numeric_cols
    ) -> CheckResult:
        cols = numeric_cols[:10]  # cap to keep query manageable
        sf_aggs = self._sf_aggregates_by_date(sf_schema, sf_table, date_col, cols)
        pg_aggs = self._pg_aggregates_by_date(
            pg_schema, pg_table, date_col.lower(), cols
        )

        all_dates = set(sf_aggs) | set(pg_aggs)
        mismatches = []
        for d in sorted(all_dates):
            sf_row = sf_aggs.get(d, {})
            pg_row = pg_aggs.get(d, {})
            for i, col in enumerate(cols):
                sf_val = self._norm_decimal(sf_row.get(i))
                pg_val = self._norm_decimal(pg_row.get(i))
                if sf_val != pg_val:
                    mismatches.append(
                        f"  {d} / {col}: SF={sf_val}  PG={pg_val}"
                    )
                    if len(mismatches) >= 30:
                        break
            if len(mismatches) >= 30:
                break

        passed = len(mismatches) == 0
        return CheckResult(
            name="aggregate_fingerprint",
            passed=passed,
            source_value=len(sf_aggs),
            target_value=len(pg_aggs),
            message=(
                f"Aggregate sums match "
                f"({len(all_dates)} partitions, {len(cols)} columns)"
                if passed
                else f"{len(mismatches)} aggregate mismatch(es) found"
            ),
            details=mismatches,
        )

    def _check_row_sample(
        self, sf_schema, sf_table, pg_schema, pg_table, columns
    ) -> CheckResult:
        pk_cols = self._get_pk_columns(sf_schema, sf_table)
        if not pk_cols:
            return CheckResult(
                name="row_sample",
                passed=None,
                source_value=0,
                target_value=0,
                message="Row sample skipped: no primary key detected on source table",
            )

        sf_rows = self._sf_tablesample(sf_schema, sf_table, self.sample_size)
        if not sf_rows:
            return CheckResult(
                name="row_sample",
                passed=True,
                source_value=0,
                target_value=0,
                message="Row sample: table appears empty",
            )

        not_found = 0
        mismatches = []
        for sf_row in sf_rows:
            pg_row = self._pg_lookup_by_pk(pg_schema, pg_table, pk_cols, sf_row)
            if pg_row is None:
                not_found += 1
                continue
            for col in columns:
                col_lower = col["name"].lower()
                sf_val = self._norm_val(
                    sf_row.get(col["name"], sf_row.get(col_lower))
                )
                pg_val = self._norm_val(pg_row.get(col_lower))
                if sf_val != pg_val:
                    pk_info = {pk: sf_row.get(pk, sf_row.get(pk.lower())) for pk in pk_cols}
                    mismatches.append(
                        f"  pk={pk_info} col={col_lower}: SF={sf_val!r}  PG={pg_val!r}"
                    )
                    if len(mismatches) >= 20:
                        break
            if len(mismatches) >= 20:
                break

        passed = not_found == 0 and len(mismatches) == 0
        details = []
        if not_found:
            details.append(f"  {not_found}/{len(sf_rows)} rows not found in Postgres")
        details.extend(mismatches)

        return CheckResult(
            name="row_sample",
            passed=passed,
            source_value=len(sf_rows),
            target_value=len(sf_rows) - not_found,
            message=(
                f"Row sample passed ({len(sf_rows):,} rows checked)"
                if passed
                else f"Row sample failed: {not_found} missing, {len(mismatches)} field mismatch(es)"
            ),
            details=details,
        )

    # ------------------------------------------------------------------
    # Snowflake query helpers
    # ------------------------------------------------------------------

    def _get_columns(self, schema: str, table: str) -> List[Dict]:
        query = """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = UPPER(%s)
        ORDER BY ORDINAL_POSITION
        """
        with self.sf_conn.cursor() as cur:
            cur.execute(query, (schema, table))
            return [
                {"name": row["COLUMN_NAME"], "type": row["DATA_TYPE"]}
                for row in cur.fetchall()
            ]

    def _get_pk_columns(self, schema: str, table: str) -> List[str]:
        try:
            query = """
            SELECT kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = %s AND tc.TABLE_NAME = UPPER(%s)
            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY kcu.ORDINAL_POSITION
            """
            with self.sf_conn.cursor() as cur:
                cur.execute(query, (schema, table))
                return [row["COLUMN_NAME"] for row in cur.fetchall()]
        except Exception:
            return []

    def _sf_scalar(self, query: str, key: str) -> Optional[int]:
        with self.sf_conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            return row[key] if row else None

    def _sf_group_count(self, schema: str, table: str, date_col: str) -> Dict[str, int]:
        query = (
            f'SELECT CAST("{date_col}" AS DATE) as D, COUNT(*) as CNT '
            f"FROM {schema}.{table} GROUP BY 1 ORDER BY 1"
        )
        with self.sf_conn.cursor() as cur:
            cur.execute(query)
            return {str(row["D"]): row["CNT"] for row in cur.fetchall()}

    def _sf_aggregates_by_date(
        self, schema: str, table: str, date_col: str, numeric_cols: List[str]
    ) -> Dict[str, Dict[int, Any]]:
        col_exprs = ", ".join(
            [f'SUM("{c}") as S{i}' for i, c in enumerate(numeric_cols)]
        )
        query = (
            f'SELECT CAST("{date_col}" AS DATE) as D, {col_exprs} '
            f"FROM {schema}.{table} GROUP BY 1 ORDER BY 1"
        )
        with self.sf_conn.cursor() as cur:
            cur.execute(query)
            return {
                str(row["D"]): {i: row[f"S{i}"] for i in range(len(numeric_cols))}
                for row in cur.fetchall()
            }

    def _sf_tablesample(self, schema: str, table: str, n: int) -> List[Dict]:
        try:
            with self.sf_conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {schema}.{table} TABLESAMPLE ({n} ROWS)")
                return cur.fetchall()
        except Exception:
            with self.sf_conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {schema}.{table} LIMIT {n}")
                return cur.fetchall()

    def _compare_null_counts(
        self,
        sf_schema: str,
        sf_table: str,
        pg_schema: str,
        pg_table: str,
        columns: List[Dict],
    ) -> List[str]:
        mismatches = []
        for chunk_start in range(0, len(columns), self._NULL_CHUNK):
            chunk = columns[chunk_start : chunk_start + self._NULL_CHUNK]

            sf_exprs = ", ".join(
                [f'COUNT(*) - COUNT("{c["name"]}") as N{i}' for i, c in enumerate(chunk)]
            )
            pg_exprs = ", ".join(
                [
                    f'COUNT(*) - COUNT("{c["name"].lower()}") as n{i}'
                    for i, c in enumerate(chunk)
                ]
            )

            with self.sf_conn.cursor() as cur:
                cur.execute(f"SELECT {sf_exprs} FROM {sf_schema}.{sf_table}")
                sf_row = cur.fetchone()

            with self.pg_conn.cursor() as cur:
                cur.execute(f'SELECT {pg_exprs} FROM {pg_schema}."{pg_table}"')
                pg_row = cur.fetchone()

            for i, col in enumerate(chunk):
                sf_nulls = sf_row.get(f"N{i}") if sf_row else None
                pg_nulls = pg_row.get(f"n{i}") if pg_row else None
                if sf_nulls != pg_nulls:
                    mismatches.append(
                        f"  {col['name']}: SF={sf_nulls:,}  PG={pg_nulls:,}"
                    )
        return mismatches

    def _compare_minmax(
        self,
        sf_schema: str,
        sf_table: str,
        pg_schema: str,
        pg_table: str,
        columns: List[Dict],
    ) -> List[str]:
        mismatches = []
        for chunk_start in range(0, len(columns), self._MINMAX_CHUNK):
            chunk = columns[chunk_start : chunk_start + self._MINMAX_CHUNK]

            sf_exprs = ", ".join(
                [
                    f'MIN("{c["name"]}") as MN{i}, MAX("{c["name"]}") as MX{i}'
                    for i, c in enumerate(chunk)
                ]
            )
            pg_exprs = ", ".join(
                [
                    f'MIN("{c["name"].lower()}") as mn{i}, MAX("{c["name"].lower()}") as mx{i}'
                    for i, c in enumerate(chunk)
                ]
            )

            with self.sf_conn.cursor() as cur:
                cur.execute(f"SELECT {sf_exprs} FROM {sf_schema}.{sf_table}")
                sf_row = cur.fetchone()

            with self.pg_conn.cursor() as cur:
                cur.execute(f'SELECT {pg_exprs} FROM {pg_schema}."{pg_table}"')
                pg_row = cur.fetchone()

            for i, col in enumerate(chunk):
                sf_mn = self._norm_val(sf_row.get(f"MN{i}") if sf_row else None)
                sf_mx = self._norm_val(sf_row.get(f"MX{i}") if sf_row else None)
                pg_mn = self._norm_val(pg_row.get(f"mn{i}") if pg_row else None)
                pg_mx = self._norm_val(pg_row.get(f"mx{i}") if pg_row else None)
                if sf_mn != pg_mn or sf_mx != pg_mx:
                    mismatches.append(
                        f"  {col['name']}: "
                        f"SF=({sf_mn}, {sf_mx})  PG=({pg_mn}, {pg_mx})"
                    )
        return mismatches

    # ------------------------------------------------------------------
    # PostgreSQL query helpers
    # ------------------------------------------------------------------

    def _pg_scalar(self, query: str, key: str) -> Optional[int]:
        with self.pg_conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            return row[key] if row else None

    def _pg_group_count(self, schema: str, table: str, date_col: str) -> Dict[str, int]:
        query = (
            f'SELECT CAST("{date_col}" AS DATE) as d, COUNT(*) as cnt '
            f'FROM {schema}."{table}" GROUP BY 1 ORDER BY 1'
        )
        with self.pg_conn.cursor() as cur:
            cur.execute(query)
            return {str(row["d"]): row["cnt"] for row in cur.fetchall()}

    def _pg_aggregates_by_date(
        self, schema: str, table: str, date_col: str, numeric_cols: List[str]
    ) -> Dict[str, Dict[int, Any]]:
        col_exprs = ", ".join(
            [f'SUM("{c.lower()}") as s{i}' for i, c in enumerate(numeric_cols)]
        )
        query = (
            f'SELECT CAST("{date_col}" AS DATE) as d, {col_exprs} '
            f'FROM {schema}."{table}" GROUP BY 1 ORDER BY 1'
        )
        with self.pg_conn.cursor() as cur:
            cur.execute(query)
            return {
                str(row["d"]): {i: row[f"s{i}"] for i in range(len(numeric_cols))}
                for row in cur.fetchall()
            }

    def _pg_lookup_by_pk(
        self,
        schema: str,
        table: str,
        pk_cols: List[str],
        sf_row: Dict,
    ) -> Optional[Dict]:
        conditions = " AND ".join([f'"{pk.lower()}" = %s' for pk in pk_cols])
        values = [sf_row.get(pk, sf_row.get(pk.lower())) for pk in pk_cols]
        try:
            with self.pg_conn.cursor() as cur:
                cur.execute(
                    f'SELECT * FROM {schema}."{table}" WHERE {conditions}', values
                )
                return cur.fetchone()
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Column classification helpers
    # ------------------------------------------------------------------

    def _detect_date_column(self, columns: List[Dict]) -> Optional[str]:
        date_types = {
            "DATE",
            "TIMESTAMP",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_LTZ",
            "TIMESTAMP_TZ",
        }
        name_hints = ("date", "day", "period", "month", "week", "year")

        # Prefer columns whose name contains a hint
        for col in columns:
            if col["type"].upper() in date_types:
                if any(hint in col["name"].lower() for hint in name_hints):
                    return col["name"]

        # Fall back to the first date-type column
        for col in columns:
            if col["type"].upper() in date_types:
                return col["name"]

        return None

    def _get_numeric_columns(self, columns: List[Dict]) -> List[str]:
        numeric_types = {
            "NUMBER", "NUMERIC", "DECIMAL",
            "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL",
            "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT",
        }
        return [
            col["name"]
            for col in columns
            if col["type"].upper().split("(")[0] in numeric_types
        ]

    def _is_numeric_or_date(self, data_type: str) -> bool:
        checkable = {
            "NUMBER", "NUMERIC", "DECIMAL",
            "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL",
            "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT",
            "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        }
        return data_type.upper().split("(")[0] in checkable

    # ------------------------------------------------------------------
    # Value normalization
    # ------------------------------------------------------------------

    def _norm_decimal(self, val: Any) -> str:
        if val is None:
            return "NULL"
        try:
            return str(Decimal(str(val)).normalize())
        except (InvalidOperation, TypeError):
            return str(val)

    def _norm_val(self, val: Any) -> str:
        if val is None:
            return "NULL"
        return str(val).strip()

    def _status(self, message: str) -> None:
        if self.status_callback:
            self.status_callback(message)
