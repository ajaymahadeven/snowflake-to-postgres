"""
snowflake_to_postgres/checkpoint.py

Persistent checkpoint for long-running migrations.

The checkpoint file is a JSON document written to disk after every committed
batch.  On restart the engine reads it to:
  - skip tables that already completed successfully
  - resume an interrupted table from the last committed row offset
    (using OFFSET on the Snowflake query)

File format:
{
  "schema":    "ENDEAVOUR_STAGING",
  "target":    "endeavour_staging",
  "completed": ["table_a", "table_b"],
  "in_progress": {
    "big_table": 5900000
  }
}
"""

import json
import threading
from pathlib import Path
from typing import Dict, List


class CheckpointManager:
    """Thread-safe checkpoint manager backed by a JSON file."""

    def __init__(self, path: str, source_schema: str = "", target_schema: str = ""):
        self.path = Path(path)
        self._lock = threading.Lock()
        self._data = self._load()
        # Stamp schema names on first creation so the file is self-describing
        if not self._data.get("schema") and source_schema:
            self._data["schema"] = source_schema
            self._data["target"] = target_schema
            self._save_unlocked()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> dict:
        if self.path.exists():
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            # Normalise older files that may lack keys
            data.setdefault("completed", [])
            data.setdefault("in_progress", {})
            return data
        return {"completed": [], "in_progress": {}}

    def _save_unlocked(self):
        """Write checkpoint to disk.  Caller must hold self._lock."""
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)
        tmp.replace(self.path)  # atomic rename

    def _save(self):
        with self._lock:
            self._save_unlocked()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_completed(self, table: str) -> bool:
        with self._lock:
            return table in self._data["completed"]

    def get_offset(self, table: str) -> int:
        """Return the number of rows already committed for *table* (0 if none)."""
        with self._lock:
            return self._data["in_progress"].get(table, 0)

    def update_progress(self, table: str, rows_committed: int):
        """Record progress after each batch commit."""
        with self._lock:
            self._data["in_progress"][table] = rows_committed
            self._save_unlocked()

    def mark_completed(self, table: str):
        """Record a table as fully transferred."""
        with self._lock:
            completed = set(self._data["completed"])
            completed.add(table)
            self._data["completed"] = sorted(completed)
            self._data["in_progress"].pop(table, None)
            self._save_unlocked()

    def completed_tables(self) -> List[str]:
        with self._lock:
            return list(self._data["completed"])

    def summary(self) -> Dict[str, int]:
        with self._lock:
            return {
                "completed": len(self._data["completed"]),
                "in_progress": len(self._data["in_progress"]),
            }
