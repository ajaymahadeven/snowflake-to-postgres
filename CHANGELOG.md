# Changelog

All notable changes to this project are documented here.

---

## [Unreleased] — 2026-03-07

### Added

- **Post-action prompts** for `migrate`, `transfer`, `build`, `validate`, and `build-views` commands:
  - **Verify prompt** (`migrate`, `transfer` only) — runs the full `validate` command inline immediately after completion so data integrity can be confirmed in the same session.
  - **Save log prompt** (all above commands) — writes the full terminal output to `logs/{YYYYMMDD_HHMMSS}_{SCHEMA}/{action}.log` in the current working directory, including start/end time, schema names, and the command that was run.
- `--no-prompt` flag to suppress post-action prompts for CI/automation without affecting the `destroy` confirmation gate that `--force` controls.
- `TeeWriter` internal class that mirrors all output to both the terminal and an in-memory buffer, enabling log capture without changing any existing output calls.

### Notes

- Prompts are suppressed when `--force`, `--no-prompt`, or `--dry-run` is passed.
- Log files are saved relative to the working directory where `manage.py` is run.

---

## 2025 — Validate Command & Stability

### Added

- **`validate` command** — 5-layer data integrity checks comparing Snowflake (source) against PostgreSQL (target):
  - Layer 1: Total row count
  - Layer 2: Per-partition row counts (grouped by date)
  - Layer 3: NULL counts + MIN/MAX per column
  - Layer 4: Aggregate sums of numeric columns per day
  - Layer 5: Row-level sample comparison (opt-in via `--sample-size`, requires a primary key)
- `--sample-size` option for `validate` to enable Layer 5 row-level sampling.

### Fixed

- Empty DDL statements no longer counted in progress totals, fixing misleading `[1/0]` style output during `build`.
- Identifiers (schema, table, column names) are now normalized to lowercase throughout schema discovery to prevent case-mismatch errors between Snowflake and PostgreSQL.
- Column case handling corrected in the data transfer engine; `--table` filter now works correctly in both discovery and transfer.

---

## 2025 — Transfer Options & Feedback

### Added

- `--where` option to filter rows during `transfer` with a SQL WHERE clause (e.g. `--where "CREATED_DATE >= '2025-01-01'"`).
- `--limit` option to cap the number of rows transferred, useful for testing.
- `--continue-on-error` option to keep processing remaining tables when one fails.
- Improved real-time feedback during transfer: per-batch row progress and per-table status messages.

---

## 2025 — Initial Release

### Added

- `migrate` — full migration: schema build + data transfer in one step.
- `build` — create tables in PostgreSQL without copying data; supports `--output` to save DDL to a file.
- `build-views` — translate and output Snowflake views and stored procedures to PostgreSQL DDL.
- `transfer` — copy data to existing PostgreSQL tables from Snowflake.
- `discover` — inspect tables, columns, and row counts in a Snowflake schema; supports `--format json`.
- `destroy` — drop a PostgreSQL schema and all its tables with a confirmation prompt.
- `--batch-size`, `--table`, `--dry-run`, `--force`, `--output`, `--db` options.
- Docker Compose setup with a persistent PostgreSQL volume.
- Pre-commit hooks for code formatting.
