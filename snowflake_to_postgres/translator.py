"""
snowflake_to_postgres/translator.py

Translates Snowflake data types and DDL to PostgreSQL equivalents.
"""

from typing import List, Optional

from .discovery import Column, Schema, Table


class SnowflakeToPostgresTypeMapper:
    """Maps Snowflake data types to PostgreSQL data types."""

    TYPE_MAPPING = {
        # Numeric types
        "NUMBER": "NUMERIC",
        "DECIMAL": "DECIMAL",
        "NUMERIC": "NUMERIC",
        "INT": "INTEGER",
        "INTEGER": "INTEGER",
        "BIGINT": "BIGINT",
        "SMALLINT": "SMALLINT",
        "TINYINT": "SMALLINT",
        "BYTEINT": "SMALLINT",
        "FLOAT": "DOUBLE PRECISION",
        "FLOAT4": "REAL",
        "FLOAT8": "DOUBLE PRECISION",
        "DOUBLE": "DOUBLE PRECISION",
        "DOUBLE PRECISION": "DOUBLE PRECISION",
        "REAL": "REAL",
        # String types
        "VARCHAR": "VARCHAR",
        "CHAR": "CHAR",
        "CHARACTER": "CHAR",
        "STRING": "TEXT",
        "TEXT": "TEXT",
        "BINARY": "BYTEA",
        "VARBINARY": "BYTEA",
        # Date/Time types
        "DATE": "DATE",
        "DATETIME": "TIMESTAMP",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP_LTZ": "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP_NTZ": "TIMESTAMP",
        "TIMESTAMP_TZ": "TIMESTAMP WITH TIME ZONE",
        # Boolean
        "BOOLEAN": "BOOLEAN",
        # Semi-structured
        "VARIANT": "JSONB",
        "OBJECT": "JSONB",
        "ARRAY": "JSONB",
    }

    def map_type(self, column: Column) -> str:
        """
        Map a Snowflake column type to PostgreSQL equivalent.
        """
        base_type = column.data_type.upper()

        # Handle parameterized types
        if base_type in self.TYPE_MAPPING:
            pg_type = self.TYPE_MAPPING[base_type]

            # Add precision/scale for numeric types
            if pg_type in ("NUMERIC", "DECIMAL") and column.numeric_precision:
                if column.numeric_scale is not None and column.numeric_scale > 0:
                    return (
                        f"{pg_type}({column.numeric_precision},{column.numeric_scale})"
                    )
                else:
                    return f"{pg_type}({column.numeric_precision})"

            # Add length for character types
            if pg_type in ("VARCHAR", "CHAR") and column.character_maximum_length:
                return f"{pg_type}({column.character_maximum_length})"

            return pg_type

        # Default to TEXT for unknown types
        return "TEXT"


class PostgresDDLGenerator:
    """Generates PostgreSQL DDL statements from schema metadata."""

    def __init__(self, type_mapper: Optional[SnowflakeToPostgresTypeMapper] = None):
        self.type_mapper = type_mapper or SnowflakeToPostgresTypeMapper()

    def generate_schema_ddl(
        self, schema: Schema, target_schema: str = None
    ) -> List[str]:
        """
        Generate complete DDL for a schema.
        """
        target_schema = target_schema or schema.name.lower()
        ddl_statements = []

        # Create schema
        ddl_statements.append(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
        ddl_statements.append("")

        # Generate table DDL (sorted by dependencies)
        sorted_tables = self._sort_tables_by_dependencies(schema.tables)

        for table in sorted_tables:
            ddl_statements.extend(self.generate_table_ddl(table, target_schema))
            ddl_statements.append("")

        # Generate foreign keys separately (after all tables exist)
        for table in schema.tables:
            fk_statements = self._generate_foreign_keys(table, target_schema)
            if fk_statements:
                ddl_statements.extend(fk_statements)
                ddl_statements.append("")

        return ddl_statements

    def generate_table_ddl(self, table: Table, schema: str) -> List[str]:
        """Generate DDL for a single table."""
        statements = []

        # Table creation
        columns_ddl = []
        for col in table.columns:
            col_def = self._generate_column_definition(col)
            columns_ddl.append(f"    {col_def}")

        # Add primary key inline if exists
        if table.primary_key:
            pk_cols = ", ".join([f'"{col}"' for col in table.primary_key.columns])
            columns_ddl.append(
                f'    CONSTRAINT "{table.primary_key.name}" PRIMARY KEY ({pk_cols})'
            )

        # Add unique constraints inline
        for constraint in table.unique_constraints:
            unique_cols = ", ".join([f'"{col}"' for col in constraint.columns])
            columns_ddl.append(
                f'    CONSTRAINT "{constraint.name}" UNIQUE ({unique_cols})'
            )

        table_ddl = f'CREATE TABLE {schema}."{table.name}" (\n'
        table_ddl += ",\n".join(columns_ddl)
        table_ddl += "\n);"

        statements.append(table_ddl)

        # Add table comment
        if table.comment:
            comment_sql = f"COMMENT ON TABLE {schema}.\"{table.name}\" IS '{self._escape_string(table.comment)}';"
            statements.append(comment_sql)

        # Add column comments
        for col in table.columns:
            if col.comment:
                comment_sql = f'COMMENT ON COLUMN {schema}."{table.name}"."{col.name}" IS \'{self._escape_string(col.comment)}\';'
                statements.append(comment_sql)

        return statements

    def _generate_column_definition(self, col: Column) -> str:
        """Generate column definition."""
        pg_type = self.type_mapper.map_type(col)

        col_def = f'"{col.name}" {pg_type}'

        if not col.is_nullable:
            col_def += " NOT NULL"

        if col.default_value:
            # Clean up default value
            default = col.default_value.strip()
            if not default.upper().startswith(("NEXTVAL", "CURRENT_")):
                col_def += f" DEFAULT {default}"

        return col_def

    def _generate_foreign_keys(self, table: Table, schema: str) -> List[str]:
        """Generate foreign key constraints."""
        statements = []

        for fk in table.foreign_keys:
            fk_cols = ", ".join([f'"{col}"' for col in fk.columns])
            ref_cols = ", ".join([f'"{col}"' for col in fk.referenced_columns])

            fk_sql = f'ALTER TABLE {schema}."{table.name}" '
            fk_sql += f'ADD CONSTRAINT "{fk.name}" '
            fk_sql += f"FOREIGN KEY ({fk_cols}) "
            fk_sql += f'REFERENCES {schema}."{fk.referenced_table}" ({ref_cols});'

            statements.append(fk_sql)

        return statements

    def _sort_tables_by_dependencies(self, tables: List[Table]) -> List[Table]:
        """
        Sort tables to respect foreign key dependencies.
        """
        table_map = {t.name: t for t in tables}
        sorted_tables = []
        visited = set()

        def visit(table: Table):
            if table.name in visited:
                return

            visited.add(table.name)

            # Visit referenced tables first
            for fk in table.foreign_keys:
                if fk.referenced_table and fk.referenced_table in table_map:
                    visit(table_map[fk.referenced_table])

            sorted_tables.append(table)

        for table in tables:
            visit(table)

        return sorted_tables

    def _escape_string(self, s: str) -> str:
        """Escape string for SQL."""
        return s.replace("'", "''")

    def generate_drop_schema_ddl(self, schema_name: str) -> List[str]:
        """Generate DDL to drop a schema and all its objects."""
        return [f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;"]
