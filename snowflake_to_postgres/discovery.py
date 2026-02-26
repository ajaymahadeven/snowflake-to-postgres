"""
snowflake_to_postgres/discovery.py

Schema discovery and introspection for Snowflake databases.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional


class ConstraintType(Enum):
    PRIMARY_KEY = "PRIMARY KEY"
    FOREIGN_KEY = "FOREIGN KEY"
    UNIQUE = "UNIQUE"
    CHECK = "CHECK"


@dataclass
class Column:
    """Represents a table column."""

    name: str
    data_type: str
    is_nullable: bool
    default_value: Optional[str] = None
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    ordinal_position: int = 0
    comment: Optional[str] = None


@dataclass
class Constraint:
    """Represents a table constraint."""

    name: str
    type: ConstraintType
    columns: List[str]
    referenced_table: Optional[str] = None
    referenced_columns: Optional[List[str]] = None
    check_clause: Optional[str] = None


@dataclass
class Index:
    """Represents a table index."""

    name: str
    columns: List[str]
    is_unique: bool
    index_type: Optional[str] = None


@dataclass
class Table:
    """Represents a database table with full metadata."""

    name: str
    schema: str
    columns: List[Column] = field(default_factory=list)
    primary_key: Optional[Constraint] = None
    foreign_keys: List[Constraint] = field(default_factory=list)
    unique_constraints: List[Constraint] = field(default_factory=list)
    check_constraints: List[Constraint] = field(default_factory=list)
    indexes: List[Index] = field(default_factory=list)
    comment: Optional[str] = None
    row_count: int = 0


@dataclass
class View:
    """Represents a database view."""

    name: str
    ddl: Optional[str] = None


@dataclass
class Procedure:
    """Represents a stored procedure."""

    name: str
    ddl: Optional[str] = None


@dataclass
class Schema:
    """Represents a complete database schema."""

    name: str
    database: str
    tables: List[Table] = field(default_factory=list)
    views: List[View] = field(default_factory=list)
    procedures: List[Procedure] = field(default_factory=list)


class SnowflakeSchemaDiscovery:
    """Discovers and extracts schema information from Snowflake."""

    def __init__(self, connection):
        self.conn = connection
        self._constraint_warning_shown = False

    def discover_schema(
        self, schema_name: str, table_filter: Optional[str] = None
    ) -> Schema:
        """
        Discover complete schema metadata from Snowflake.

        Args:
            schema_name: Name of the schema to discover
            table_filter: If provided, only discover this specific table

        Returns:
            Schema object with all metadata
        """
        schema = Schema(name=schema_name, database=self.conn.config["database"])

        # Get tables (filtered if specified)
        tables = self._get_tables(schema_name)
        if table_filter:
            table_filter_upper = table_filter.upper()
            tables = [t for t in tables if t.upper() == table_filter_upper]
            if not tables:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Table '{table_filter}' not found in schema '{schema_name}'. "
                    f"No tables will be processed."
                )

        for table_name in tables:
            table = Table(name=table_name.lower(), schema=schema_name.lower())

            # Get columns
            table.columns = self._get_columns(schema_name, table_name)

            # Get constraints
            constraints = self._get_constraints(schema_name, table_name)
            for constraint in constraints:
                if constraint.type == ConstraintType.PRIMARY_KEY:
                    table.primary_key = constraint
                elif constraint.type == ConstraintType.FOREIGN_KEY:
                    table.foreign_keys.append(constraint)
                elif constraint.type == ConstraintType.UNIQUE:
                    table.unique_constraints.append(constraint)
                elif constraint.type == ConstraintType.CHECK:
                    table.check_constraints.append(constraint)

            # Get row count estimate
            table.row_count = self._get_row_count(schema_name, table_name)

            # Get table comment
            table.comment = self._get_table_comment(schema_name, table_name)

            schema.tables.append(table)

        # Skip views and procedures when filtering by specific table
        if not table_filter:
            # Get views with definitions
            view_names = self._get_views(schema_name)
            for view_name in view_names:
                view_ddl = self._get_view_definition(schema_name, view_name)
                schema.views.append(View(name=view_name, ddl=view_ddl))

            # Get procedures with definitions
            procedure_names = self._get_procedures(schema_name)
            for proc_name in procedure_names:
                proc_ddl = self._get_procedure_definition(schema_name, proc_name)
                schema.procedures.append(Procedure(name=proc_name, ddl=proc_ddl))

        return schema

    def _get_tables(self, schema_name: str) -> List[str]:
        """Get all table names in schema."""
        query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema_name,))
            return [row["TABLE_NAME"] for row in cur.fetchall()]

    def _get_columns(self, schema_name: str, table_name: str) -> List[Column]:
        """Get all columns for a table."""
        query = """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            ORDINAL_POSITION,
            COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema_name, table_name))
            columns = []
            for row in cur.fetchall():
                columns.append(
                    Column(
                        name=row["COLUMN_NAME"].lower(),
                        data_type=row["DATA_TYPE"],
                        is_nullable=row["IS_NULLABLE"] == "YES",
                        default_value=row["COLUMN_DEFAULT"],
                        character_maximum_length=row["CHARACTER_MAXIMUM_LENGTH"],
                        numeric_precision=row["NUMERIC_PRECISION"],
                        numeric_scale=row["NUMERIC_SCALE"],
                        ordinal_position=row["ORDINAL_POSITION"],
                        comment=row["COMMENT"],
                    )
                )
            return columns

    def _get_constraints(self, schema_name: str, table_name: str) -> List[Constraint]:
        """Get all constraints for a table."""
        constraints = []

        # Try to get primary keys and unique constraints
        try:
            query = """
            SELECT
                tc.CONSTRAINT_NAME,
                tc.CONSTRAINT_TYPE,
                kcu.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = %s
            AND tc.TABLE_NAME = %s
            AND tc.CONSTRAINT_TYPE IN ('PRIMARY KEY', 'UNIQUE')
            ORDER BY tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
            """

            constraint_map = {}

            with self.conn.cursor() as cur:
                cur.execute(query, (schema_name, table_name))
                for row in cur.fetchall():
                    constraint_name = row["CONSTRAINT_NAME"].lower()
                    if constraint_name not in constraint_map:
                        constraint_type = (
                            ConstraintType.PRIMARY_KEY
                            if row["CONSTRAINT_TYPE"] == "PRIMARY KEY"
                            else ConstraintType.UNIQUE
                        )
                        constraint_map[constraint_name] = Constraint(
                            name=constraint_name, type=constraint_type, columns=[]
                        )
                    constraint_map[constraint_name].columns.append(row["COLUMN_NAME"].lower())

                constraints.extend(constraint_map.values())
        except Exception as e:
            if not self._constraint_warning_shown:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Could not fetch constraints (INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                    f"not accessible). Constraints will be skipped."
                )
                self._constraint_warning_shown = True

        # Try to get foreign keys
        try:
            fk_query = """
            SELECT
                rc.CONSTRAINT_NAME,
                kcu.COLUMN_NAME,
                kcu2.TABLE_NAME as REFERENCED_TABLE_NAME,
                kcu2.COLUMN_NAME as REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                ON rc.UNIQUE_CONSTRAINT_NAME = kcu2.CONSTRAINT_NAME
                AND rc.UNIQUE_CONSTRAINT_SCHEMA = kcu2.CONSTRAINT_SCHEMA
            WHERE rc.CONSTRAINT_SCHEMA = %s
            AND kcu.TABLE_NAME = %s
            ORDER BY rc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
            """

            fk_map = {}
            with self.conn.cursor() as cur:
                cur.execute(fk_query, (schema_name, table_name))
                for row in cur.fetchall():
                    constraint_name = row["CONSTRAINT_NAME"].lower()
                    if constraint_name not in fk_map:
                        fk_map[constraint_name] = Constraint(
                            name=constraint_name,
                            type=ConstraintType.FOREIGN_KEY,
                            columns=[],
                            referenced_table=row["REFERENCED_TABLE_NAME"].lower(),
                            referenced_columns=[],
                        )
                    fk_map[constraint_name].columns.append(row["COLUMN_NAME"].lower())
                    fk_map[constraint_name].referenced_columns.append(
                        row["REFERENCED_COLUMN_NAME"].lower()
                    )

                constraints.extend(fk_map.values())
        except Exception:
            # Already warned about KEY_COLUMN_USAGE above; silently skip FK fetch
            pass

        return constraints

    def _get_row_count(self, schema_name: str, table_name: str) -> int:
        """Get approximate row count."""
        query = f"SELECT COUNT(*) as cnt FROM {schema_name}.{table_name}"
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
                return result["CNT"] if result else 0
        except Exception:
            return 0

    def _get_table_comment(self, schema_name: str, table_name: str) -> Optional[str]:
        """Get table comment."""
        query = """
        SELECT COMMENT
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema_name, table_name))
            result = cur.fetchone()
            return result["COMMENT"] if result and result["COMMENT"] else None

    def _get_views(self, schema_name: str) -> List[str]:
        """Get all view names in schema."""
        query = """
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (schema_name,))
            return [row["TABLE_NAME"].lower() for row in cur.fetchall()]

    def _get_view_definition(self, schema_name: str, view_name: str) -> Optional[str]:
        """Get view DDL definition."""
        query = """
        SELECT GET_DDL('VIEW', %s || '.' || %s) as DDL
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (schema_name, view_name))
                result = cur.fetchone()
                return result["DDL"] if result else None
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f"Could not fetch view definition for {schema_name}.{view_name}: {e}"
            )
            return None

    def _get_procedure_definition(
        self, schema_name: str, procedure_name: str
    ) -> Optional[str]:
        """Get procedure DDL definition."""
        query = """
        SELECT GET_DDL('PROCEDURE', %s || '.' || %s) as DDL
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (schema_name, procedure_name))
                result = cur.fetchone()
                return result["DDL"] if result else None
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(
                f"Could not fetch procedure definition for {schema_name}.{procedure_name}: {e}"
            )
            return None

    def _get_procedures(self, schema_name: str) -> List[str]:
        """Get all procedure names in schema."""
        query = """
        SELECT PROCEDURE_NAME
        FROM INFORMATION_SCHEMA.PROCEDURES
        WHERE PROCEDURE_SCHEMA = %s
        ORDER BY PROCEDURE_NAME
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (schema_name,))
                return [row["PROCEDURE_NAME"].lower() for row in cur.fetchall()]
        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Could not fetch procedures: {e}")
            return []
