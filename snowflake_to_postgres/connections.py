"""
snowflake_to_postgres/connections.py

Database connection management for Snowflake and PostgreSQL.
"""

import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import snowflake.connector
from django.conf import settings
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from snowflake.connector import DictCursor


class SnowflakeConnection:
    """Manages Snowflake database connections."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._load_config()
        self._connection = None

    def _load_config(self) -> Dict[str, Any]:
        """Load Snowflake configuration from environment/settings."""
        return {
            "user": os.getenv(
                "SNOWFLAKE_USER", getattr(settings, "SNOWFLAKE_USER", None)
            ),
            "password": os.getenv(
                "SNOWFLAKE_PASSWORD", getattr(settings, "SNOWFLAKE_PASSWORD", None)
            ),
            "account": os.getenv(
                "SNOWFLAKE_ACCOUNT", getattr(settings, "SNOWFLAKE_ACCOUNT", None)
            ),
            "warehouse": os.getenv(
                "SNOWFLAKE_WAREHOUSE", getattr(settings, "SNOWFLAKE_WAREHOUSE", None)
            ),
            "database": os.getenv(
                "SNOWFLAKE_DATABASE", getattr(settings, "SNOWFLAKE_DATABASE", None)
            ),
        }

    def connect(self):
        """Establish connection to Snowflake."""
        if self._connection is None:
            self._connection = snowflake.connector.connect(
                user=self.config["user"],
                password=self.config["password"],
                account=self.config["account"],
                warehouse=self.config["warehouse"],
                database=self.config["database"],
            )
        return self._connection

    @contextmanager
    def cursor(self, dict_cursor=True):
        """Context manager for cursor operations."""
        conn = self.connect()
        cursor_class = DictCursor if dict_cursor else None
        cur = conn.cursor(cursor_class)
        try:
            yield cur
        finally:
            cur.close()

    def close(self):
        """Close the connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class PostgresConnection:
    """Manages PostgreSQL database connections with pooling."""

    def __init__(
        self, db_alias: str = "data_factory_ops", min_conn: int = 1, max_conn: int = 5
    ):
        self.db_alias = db_alias
        self.config = self._load_config()
        self._pool = None
        self.min_conn = min_conn
        self.max_conn = max_conn

    def _load_config(self) -> Dict[str, Any]:
        """Load PostgreSQL configuration from Django settings."""
        db_config = settings.DATABASES.get(self.db_alias)
        if not db_config:
            raise ValueError(
                f"Database alias '{self.db_alias}' not found in settings.DATABASES"
            )

        return {
            "dbname": db_config["NAME"],  
            "user": db_config["USER"],
            "password": db_config["PASSWORD"],
            "host": db_config["HOST"], 
            "port": db_config.get("PORT", 5432),
        }

    def get_pool(self):
        """Get or create connection pool."""
        if self._pool is None:
            self._pool = SimpleConnectionPool(
                self.min_conn, self.max_conn, **self.config
            )
        return self._pool

    @contextmanager
    def connection(self):
        """Context manager for connection from pool."""
        pool = self.get_pool()
        conn = pool.getconn()
        try:
            yield conn
        finally:
            pool.putconn(conn)

    @contextmanager
    def cursor(self, dict_cursor=True):
        """Context manager for cursor operations."""
        with self.connection() as conn:
            cursor_class = RealDictCursor if dict_cursor else None
            cur = conn.cursor(cursor_factory=cursor_class)
            try:
                yield cur
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
            finally:
                cur.close()

    def close_pool(self):
        """Close all connections in pool."""
        if self._pool:
            self._pool.closeall()
            self._pool = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_pool()
