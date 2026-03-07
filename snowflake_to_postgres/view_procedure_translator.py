"""
snowflake_to_postgres/view_procedure_translator.py

Translates Snowflake views and procedures to PostgreSQL equivalents.
"""

import re
from typing import List, Optional, Tuple


class SnowflakeViewTranslator:
    """Translates Snowflake view DDL to PostgreSQL."""

    def translate_view(
        self, view_name: str, snowflake_ddl: str, target_schema: str
    ) -> Tuple[Optional[str], Optional[str], List[str]]:
        """
        Translate Snowflake view DDL to PostgreSQL.

        Returns:
            (pg_ddl, failure_reason, cross_schema_deps)
            - pg_ddl is None on failure
            - failure_reason is None on success
            - cross_schema_deps lists any schema.table refs outside target_schema
        """
        if not snowflake_ddl:
            return None, "Snowflake returned no DDL for this view", []

        try:
            # Extract the view body (everything after AS).
            # Handles variations Snowflake GET_DDL produces:
            #   CREATE OR REPLACE [SECURE] [RECURSIVE] VIEW [DB.]SCHEMA.VIEW [(col,...)] AS ...
            #   Quoted identifiers: "SCHEMA"."VIEW_NAME"
            _ident = r'(?:"[^"]*"|[\w$]+)'
            pattern = (
                r"create\s+(?:or\s+replace\s+)?"
                r"(?:secure\s+)?(?:recursive\s+)?"
                r"view\s+" + _ident + r"(?:\." + _ident + r")*"
                # optional column-alias list: ( "COL1", "COL2", ... )
                + r"(?:\s*\([^)]*\))?" + r"\s+as\s+(.*)"
            )
            match = re.search(pattern, snowflake_ddl, re.IGNORECASE | re.DOTALL)

            if not match:
                first_line = snowflake_ddl.strip().splitlines()[0][:120]
                return (
                    None,
                    f"DDL pattern not recognised — first line: {first_line!r}",
                    [],
                )

            view_query = match.group(1).strip()

            # 1. Strip Snowflake 3-part DB.schema.table references (before cross-schema detection)
            view_query = self._strip_db_prefix(view_query)

            # 2. Detect cross-schema refs (FROM/JOIN only, avoids alias.column false positives)
            cross_schema_deps = self._detect_cross_schema_refs(
                view_query, target_schema
            )

            # 3. Qualify bare table references with target_schema
            view_query = self._qualify_bare_tables(view_query, target_schema)

            # 4. Translate IFF(cond, true, false) → CASE WHEN cond THEN true ELSE false END
            view_query = self._translate_iff(view_query)

            # 5. Replace other Snowflake-specific functions/syntax
            view_query = self._replace_functions(view_query)

            # 6. Fix lpad/rpad numeric first-arg (PostgreSQL requires text)
            view_query = self._fix_lpad_rpad(view_query)

            pg_ddl = (
                f'CREATE OR REPLACE VIEW {target_schema}."{view_name}" AS\n{view_query}'
            )
            return pg_ddl, None, cross_schema_deps

        except Exception as e:
            return None, f"Unexpected error during translation: {e}", []

    def _strip_db_prefix(self, sql: str) -> str:
        """
        Strip Snowflake 3-part "DB"."SCHEMA"."TABLE" references down to schema."table".
        Snowflake allows cross-database refs that don't translate to PostgreSQL.
        """
        return re.sub(
            r'"[^"]+"\."([^"]+)"\."([^"]+)"',
            lambda m: f'{m.group(1).lower()}."{m.group(2).lower()}"',
            sql,
        )

    def _qualify_bare_tables(self, sql: str, target_schema: str) -> str:
        """
        Prefix unqualified table references after FROM / JOIN with target_schema.
        Skips subqueries, already-qualified refs, and CTE names.
        """
        # Collect CTE names so we don't qualify them
        cte_names = {
            name.lower()
            for name in re.findall(r"\b(\w+)\s+AS\s*\(", sql, re.IGNORECASE)
        }

        def _qualify(m):
            keyword = m.group(1)
            table = m.group(2)
            if table.lower() in cte_names:
                return m.group(0)
            # Lowercase table name: PostgreSQL tables are created lowercase
            return f'{keyword} {target_schema}."{table.lower()}"'

        # Match FROM/JOIN followed by a bare identifier (no leading/trailing dot).
        # The \b after the group prevents backtracking into partial words when the
        # negative lookahead (?!\s*\.) rejects a schema-qualified name like whsmith_core.table.
        return re.sub(
            r"\b(FROM|JOIN)\s+(?!\()([a-zA-Z_]\w*)\b(?!\s*\.)",
            _qualify,
            sql,
            flags=re.IGNORECASE,
        )

    def _detect_cross_schema_refs(self, sql: str, target_schema: str) -> List[str]:
        """
        Return a sorted list of schema.table references in FROM/JOIN clauses that
        belong to schemas other than target_schema. Only inspects FROM/JOIN to avoid
        false positives from alias.column patterns in SELECT/WHERE/ON clauses.
        """
        _skip = {"information_schema", "pg_catalog", target_schema.lower()}
        seen: set = set()
        deps: List[str] = []
        # Only match schema.table after FROM/JOIN keywords — not alias.column elsewhere
        for m in re.finditer(
            r'\b(?:FROM|JOIN)\s+([a-zA-Z_]\w+)\.((?:"[^"]+"|\w+))',
            sql,
            re.IGNORECASE,
        ):
            schema_part = m.group(1).lower()
            if schema_part in _skip:
                continue
            ref = f"{m.group(1)}.{m.group(2)}"
            if ref.lower() not in seen:
                seen.add(ref.lower())
                deps.append(ref)
        return sorted(deps)

    def _translate_iff(self, sql: str) -> str:
        """
        Translate IFF(cond, true_val, false_val) → CASE WHEN cond THEN true_val ELSE false_val END.
        Handles nested parentheses and string literals correctly.
        """
        pattern = re.compile(r"\bIFF\s*\(", re.IGNORECASE)
        result = []
        last_end = 0
        for m in pattern.finditer(sql):
            result.append(sql[last_end : m.start()])
            pos = m.end()  # position after opening '('
            args, pos = self._parse_function_args(sql, pos, 3)
            last_end = pos
            if len(args) == 3:
                # Recursively translate nested IFF in each argument
                args = [self._translate_iff(a) for a in args]
                result.append(f"CASE WHEN {args[0]} THEN {args[1]} ELSE {args[2]} END")
            else:
                result.append(f'IFF({", ".join(args)})')
        result.append(sql[last_end:])
        return "".join(result)

    def _fix_lpad_rpad(self, sql: str) -> str:
        """
        Add ::text cast to the first argument of lpad/rpad when it's not already text.
        PostgreSQL's lpad/rpad require a text first argument.
        """
        pattern = re.compile(r"\b(lpad|rpad)\s*\(", re.IGNORECASE)
        result = []
        last_end = 0
        for m in pattern.finditer(sql):
            result.append(sql[last_end : m.start()])
            pos = m.end()  # position after opening '('
            # Extract just the first argument
            first_arg_chars = []
            depth = 1
            in_string = False
            string_char = None
            first_arg_end = None
            while pos < len(sql):
                c = sql[pos]
                if in_string:
                    if c == string_char:
                        if pos + 1 < len(sql) and sql[pos + 1] == string_char:
                            first_arg_chars.extend([c, c])
                            pos += 2
                            continue
                        in_string = False
                    first_arg_chars.append(c)
                elif c in ("'", '"'):
                    in_string = True
                    string_char = c
                    first_arg_chars.append(c)
                elif c == "(":
                    depth += 1
                    first_arg_chars.append(c)
                elif c == ")":
                    depth -= 1
                    if depth == 0:
                        break
                    first_arg_chars.append(c)
                elif c == "," and depth == 1:
                    first_arg_end = pos
                    break
                else:
                    first_arg_chars.append(c)
                pos += 1

            if first_arg_end is None:
                # Couldn't parse - output as-is
                result.append(sql[m.start() : pos])
                last_end = pos
                continue

            first_arg = "".join(first_arg_chars).strip()
            is_string = first_arg.startswith("'") or first_arg.startswith('"')
            already_cast = bool(
                re.search(r"::(text|varchar|char)\b", first_arg, re.IGNORECASE)
            )

            result.append(m.group(0))
            if is_string or already_cast:
                result.append(first_arg)
            else:
                result.append(f"({first_arg})::text")
            last_end = first_arg_end  # comma and rest of args remain

        result.append(sql[last_end:])
        return "".join(result)

    def _parse_function_args(self, sql: str, pos: int, max_args: int) -> tuple:
        """
        Parse up to max_args comma-separated arguments from pos (after opening paren).
        Returns (args_list, end_pos) where end_pos is after the closing paren.
        Respects nested parens and string literals.
        """
        args = []
        current = []
        depth = 1
        in_string = False
        string_char = None
        while pos < len(sql) and depth > 0:
            c = sql[pos]
            if in_string:
                if c == string_char:
                    if pos + 1 < len(sql) and sql[pos + 1] == string_char:
                        current.extend([c, c])
                        pos += 2
                        continue
                    in_string = False
                current.append(c)
            elif c in ("'", '"'):
                in_string = True
                string_char = c
                current.append(c)
            elif c == "(":
                depth += 1
                current.append(c)
            elif c == ")":
                depth -= 1
                if depth == 0:
                    args.append("".join(current).strip())
                else:
                    current.append(c)
            elif c == "," and depth == 1 and len(args) < max_args - 1:
                args.append("".join(current).strip())
                current = []
            else:
                current.append(c)
            pos += 1
        return args, pos

    def _replace_functions(self, sql: str) -> str:
        """Replace Snowflake-specific functions with PostgreSQL equivalents."""
        result = sql

        # Strip LATERAL SPLIT_TO_TABLE(col, delim) alias → LATERAL UNNEST(STRING_TO_ARRAY(col, delim)) AS alias(value)
        result = re.sub(
            r"\blateral\s+split_to_table\((\w+(?:\.\w+)?),\s*([^)]+)\)\s+(\w+)",
            r"lateral unnest(string_to_array(\1, \2)) AS \3(value)",
            result,
            flags=re.IGNORECASE,
        )
        # Generic SPLIT_TO_TABLE without lateral
        result = re.sub(
            r"\bsplit_to_table\((\w+(?:\.\w+)?),\s*([^)]+)\)",
            r"unnest(string_to_array(\1, \2))",
            result,
            flags=re.IGNORECASE,
        )

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
            r"\bNVL\(": "COALESCE(",
            r"\bIFNULL\(": "COALESCE(",
            # Type casts
            r"::VARCHAR": "::TEXT",
            r"::STRING": "::TEXT",
            r"::NUMBER": "::NUMERIC",
        }

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
