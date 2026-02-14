"""CSV file loader for PostgreSQL tables."""

import csv
import logging
import re
from pathlib import Path
from typing import Any

from psycopg import sql

from postgres_mcp.csv_loader.date_detector import DateDetector
from postgres_mcp.sql.sql_driver import SqlDriver

logger = logging.getLogger(__name__)

IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, label: str) -> None:
    """Validate that a name is a safe SQL identifier."""
    if not IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid {label}: '{name}'. Must match ^[a-zA-Z_][a-zA-Z0-9_]*$")


def _sanitize_column_name(name: str) -> str:
    """Sanitize a column name for use as a SQL identifier.

    - Replace non-alphanumeric chars (except underscore) with underscore
    - Prefix with 'col_' if starts with a digit
    - Lowercase
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name.strip()).lower()
    if not sanitized or sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    # Collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    if not sanitized:
        sanitized = "col"
    return sanitized


def _deduplicate_columns(columns: list[str]) -> list[str]:
    """Ensure column names are unique by appending _1, _2, etc. for duplicates."""
    seen: dict[str, int] = {}
    result: list[str] = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            result.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            result.append(col)
    return result


class CsvLoader:
    """Loads CSV files into PostgreSQL tables."""

    def __init__(self, sql_driver: SqlDriver):
        self.sql_driver = sql_driver

    async def load_csv(
        self,
        csv_path: str,
        schema: str,
        table_name: str,
        has_header: bool = True,
        detect_dates: bool = True,
        delimiter: str = ",",
        encoding: str = "utf-8",
    ) -> dict[str, Any]:
        """Load a CSV file into a new PostgreSQL table.

        Args:
            csv_path: Path to the CSV file on the server filesystem.
            schema: Target schema name.
            table_name: Name for the new table.
            has_header: Whether the CSV has a header row.
            detect_dates: Whether to auto-detect date columns as TIMESTAMP.
            delimiter: CSV delimiter character.
            encoding: File encoding.

        Returns:
            Summary dict with rows_inserted, column_count, date_columns.
        """
        # 1. Validate inputs
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        _validate_identifier(schema, "schema name")
        _validate_identifier(table_name, "table name")

        # 2. Read header / determine columns
        with open(path, newline="", encoding=encoding) as f:
            reader = csv.reader(f, delimiter=delimiter)
            first_row = next(reader, None)
            if first_row is None:
                raise ValueError("CSV file is empty")

        if has_header:
            columns = _deduplicate_columns([_sanitize_column_name(c) for c in first_row])
        else:
            columns = [f"col_{i + 1}" for i in range(len(first_row))]

        # 3. Detect date columns
        date_columns: set[str] = set()
        if detect_dates:
            date_columns = DateDetector.detect_date_columns(
                csv_path=csv_path,
                has_header=has_header,
                delimiter=delimiter,
                encoding=encoding,
            )
            # Map detected header names to sanitized names
            if has_header:
                sanitized_map = {}
                with open(path, newline="", encoding=encoding) as f:
                    reader = csv.reader(f, delimiter=delimiter)
                    raw_header = next(reader, [])
                for raw, sanitized in zip(raw_header, columns):
                    if raw in date_columns:
                        sanitized_map[sanitized] = True
                date_columns = set(sanitized_map.keys())

        # 4. Check table doesn't already exist
        qualified_name = f"{schema}.{table_name}"
        check_result = await self.sql_driver.execute_query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            [schema, table_name],
        )
        if check_result:
            raise ValueError(f"Table {qualified_name} already exists")

        # 5. Build column types
        col_types = []
        for col in columns:
            col_type = "TIMESTAMP" if col in date_columns else "TEXT"
            col_types.append(col_type)

        # 6. Create table using sql.Identifier for injection safety
        col_defs = sql.SQL(", ").join(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type)) for col, col_type in zip(columns, col_types))
        create_query = sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(schema),
            sql.Identifier(table_name),
            col_defs,
        )

        # Execute DDL via direct pool access (execute_query expects LiteralString)
        db_conn = self.sql_driver.connect()
        pool = await db_conn.pool_connect()
        async with pool.connection() as conn:
            await conn.execute(create_query)
            await conn.commit()

        # 7. Load data using COPY protocol
        rows_inserted = 0
        try:
            async with pool.connection() as conn:
                copy_query = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                )

                async with conn.cursor() as cur:
                    async with cur.copy(copy_query) as copy:
                        with open(path, newline="", encoding=encoding) as f:
                            reader = csv.reader(f, delimiter=delimiter)
                            if has_header:
                                next(reader)  # Skip header

                            for row in reader:
                                # Transform date values to ISO format
                                transformed = []
                                for i, val in enumerate(row):
                                    if i < len(columns) and columns[i] in date_columns and val.strip():
                                        dt = DateDetector.parse_date(val)
                                        if dt is not None:
                                            transformed.append(dt.isoformat())
                                        else:
                                            transformed.append(val)
                                    else:
                                        transformed.append(val)

                                await copy.write_row(transformed)
                                rows_inserted += 1

                await conn.commit()
        except Exception:
            logger.error(f"Error during COPY to {qualified_name}. Partial data may remain — DROP the table to clean up.")
            raise

        return {
            "rows_inserted": rows_inserted,
            "column_count": len(columns),
            "columns": columns,
            "date_columns": sorted(date_columns),
            "table": qualified_name,
        }
