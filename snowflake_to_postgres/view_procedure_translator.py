"""
snowflake_to_postgres/view_procedure_translator.py

Translates Snowflake views and procedures to PostgreSQL equivalents.
"""

import re
from typing import Optional


class SnowflakeViewTranslator:
    """Translates Snowflake view DDL to PostgreSQL."""

    def translate_view(
        self, view_name: str, snowflake_ddl: str, target_schema: str
    ) -> Optional[str]:
        """
        Translate Snowflake view DDL to PostgreSQL.

        Args:
            view_name: Name of the view
            snowflake_ddl: Original Snowflake DDL
            target_schema: Target PostgreSQL schema

        Returns:
            PostgreSQL-compatible DDL or None if translation fails
        """
        if not snowflake_ddl:
            return None

        try:
            # Extract the view definition (everything after CREATE VIEW)
            # Snowflake format: create or replace view SCHEMA.VIEW as SELECT ...
            pattern = r"create\s+(?:or\s+replace\s+)?view\s+[\w.]+\s+as\s+(.*)"
            match = re.search(pattern, snowflake_ddl, re.IGNORECASE | re.DOTALL)

            if not match:
                return None

            view_query = match.group(1)

            # Replace Snowflake-specific functions
            view_query = self._replace_functions(view_query)

            # Build PostgreSQL DDL
            pg_ddl = (
                f'CREATE OR REPLACE VIEW {target_schema}."{view_name}" AS\n{view_query}'
            )

            return pg_ddl

        except Exception as e:
            import logging

            logger = logging.getLogger(__name__)
            logger.error(f"Failed to translate view {view_name}: {e}")
            return None

    def _replace_functions(self, sql: str) -> str:
        """Replace Snowflake-specific functions with PostgreSQL equivalents."""
        replacements = {
            # Date functions
            r"\bCURRENT_TIMESTAMP\(\)": "CURRENT_TIMESTAMP",
            r"\bGETDATE\(\)": "CURRENT_TIMESTAMP",
            r"\bSYSDATE\(\)": "CURRENT_TIMESTAMP",
            r"\bTO_DATE\(": "TO_TIMESTAMP(",
            r"\bDATE_TRUNC\(": "DATE_TRUNC(",
            r"\bDATEDIFF\(": "DATE_PART(",  # May need manual adjustment
            # String functions
            r"\bCONCAT_WS\(": "CONCAT_WS(",
            r"\bIFF\(": "CASE WHEN",  # Convert IFF to CASE
            r"\bNVL\(": "COALESCE(",
            r"\bIFNULL\(": "COALESCE(",
            # Type casts
            r"::VARCHAR": "::TEXT",
            r"::STRING": "::TEXT",
            r"::NUMBER": "::NUMERIC",
        }

        result = sql
        for pattern, replacement in replacements.items():
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

        return result


class SnowflakeProcedureTranslator:
    """Translates Snowflake procedures to PostgreSQL functions."""

    def translate_procedure(
        self, proc_name: str, snowflake_ddl: str, target_schema: str
    ) -> Optional[str]:
        """
        Translate Snowflake procedure to PostgreSQL function.

        Note: Snowflake procedures use JavaScript or SQL scripting.
        PostgreSQL uses PL/pgSQL. This may require significant manual adjustment.

        Args:
            proc_name: Name of the procedure
            snowflake_ddl: Original Snowflake DDL
            target_schema: Target PostgreSQL schema

        Returns:
            PostgreSQL function DDL (may need manual editing)
        """
        if not snowflake_ddl:
            return None

        # This is a placeholder - procedures often need manual translation
        # Snowflake supports JavaScript procedures which don't translate directly

        comment = f"""
-- WARNING: This procedure requires manual translation
-- Original Snowflake DDL:
-- {snowflake_ddl}
--
-- TODO: Rewrite as PostgreSQL PL/pgSQL function
-- CREATE OR REPLACE FUNCTION {target_schema}.{proc_name}()
-- RETURNS void AS $$
-- BEGIN
--     -- Add your logic here
-- END;
-- $$ LANGUAGE plpgsql;
"""

        return comment
