"""Temporal query and revert functionality for versioned PostgreSQL tables.

This module provides functionality to query data as it existed at specific points in time
and to revert tables to previous states.
"""

import logging
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ChangeRecord:
    """A single change record from history."""

    temporal_id: int
    operation: str
    valid_from: str
    tx_id: int
    data: Dict[str, Any]


class TemporalQuery:
    """Handles temporal queries and data reversion."""

    def __init__(self, sql_driver: Any):
        """Initialize the temporal query handler.

        Args:
            sql_driver: SqlDriver instance for database operations
        """
        self.sql_driver = sql_driver

    async def _get_history_table(self, schema_name: str, table_name: str) -> Optional[str]:
        """Get the history table name for a versioned table.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table

        Returns:
            Qualified history table name or None if not versioned
        """
        result_query = await self.sql_driver.execute_query(
            """
            SELECT history_table_name FROM temporal_versioning.versioned_tables
            WHERE schema_name = %s AND table_name = %s AND enabled = TRUE
            """,
            [schema_name, table_name],
        )
        result = result_query[0].cells if result_query else None

        if not result:
            return None

        return f"temporal_versioning.{result['history_table_name']}"

    async def _get_table_columns(self, schema_name: str, table_name: str) -> List[str]:
        """Get the column names for a table (excluding temporal metadata columns).

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table

        Returns:
            List of column names
        """
        columns_query = await self.sql_driver.execute_query(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            [schema_name, table_name],
        )
        columns_result = [row.cells for row in columns_query] if columns_query else []

        return [col["column_name"] for col in columns_result]

    async def query_at_timestamp(self, schema_name: str, table_name: str, timestamp: str, limit: int = 100) -> Dict[str, Any]:
        """Query table data as it existed at a specific timestamp.

        This reconstructs the table state by finding the most recent change before the timestamp
        for each row.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table
            timestamp: ISO timestamp (e.g., "2024-01-15 10:30:00")
            limit: Maximum number of rows to return (default: 100)

        Returns:
            Dict with query results and metadata
        """
        history_table = await self._get_history_table(schema_name, table_name)
        if not history_table:
            raise ValueError(f"Table {schema_name}.{table_name} is not temporally versioned")

        # Get the base table columns (excluding temporal metadata)
        columns = await self._get_table_columns(schema_name, table_name)
        column_list = ", ".join(columns)

        # Query the history table for the most recent state before the timestamp
        # This uses a window function to get the latest change for each unique row
        query = f"""
        WITH latest_changes AS (
            SELECT
                {column_list},
                temporal_operation,
                temporal_valid_from,
                ROW_NUMBER() OVER (
                    PARTITION BY {columns[0] if columns else "temporal_id"}
                    ORDER BY temporal_valid_from DESC
                ) as rn
            FROM {history_table}
            WHERE temporal_valid_from <= %s::timestamp
        )
        SELECT {column_list}, temporal_operation, temporal_valid_from::text as temporal_valid_from
        FROM latest_changes
        WHERE rn = 1 AND temporal_operation != 'DELETE'
        ORDER BY temporal_valid_from DESC
        LIMIT %s
        """

        results_query = await self.sql_driver.execute_query(query, [timestamp, limit])
        results = [row.cells for row in results_query] if results_query else []

        return {
            "schema_name": schema_name,
            "table_name": table_name,
            "timestamp": timestamp,
            "row_count": len(results),
            "rows": results,
        }

    async def get_change_history(
        self,
        schema_name: str,
        table_name: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        operation: Optional[str] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Get change history for a table within a time range.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table
            start_time: Start of time range (ISO timestamp, optional)
            end_time: End of time range (ISO timestamp, optional)
            operation: Filter by operation type ('INSERT', 'UPDATE', 'DELETE', optional)
            limit: Maximum number of changes to return (default: 100)

        Returns:
            Dict with change history and metadata
        """
        history_table = await self._get_history_table(schema_name, table_name)
        if not history_table:
            raise ValueError(f"Table {schema_name}.{table_name} is not temporally versioned")

        # Build WHERE clause conditions
        conditions = []
        params = []

        if start_time:
            conditions.append("temporal_valid_from >= %s::timestamp")
            params.append(start_time)

        if end_time:
            conditions.append("temporal_valid_from <= %s::timestamp")
            params.append(end_time)

        if operation:
            conditions.append("temporal_operation = %s")
            params.append(operation.upper())

        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        # Get the base table columns
        columns = await self._get_table_columns(schema_name, table_name)
        column_list = ", ".join(columns)

        query = f"""
        SELECT
            temporal_id,
            temporal_operation,
            temporal_valid_from::text as temporal_valid_from,
            temporal_tx_id,
            {column_list}
        FROM {history_table}
        WHERE {where_clause}
        ORDER BY temporal_valid_from DESC, temporal_id DESC
        LIMIT %s
        """

        params.append(limit)
        results_query = await self.sql_driver.execute_query(query, params)
        results = [row.cells for row in results_query] if results_query else []

        return {
            "schema_name": schema_name,
            "table_name": table_name,
            "start_time": start_time,
            "end_time": end_time,
            "operation_filter": operation,
            "change_count": len(results),
            "changes": results,
        }

    async def revert_to_timestamp(self, schema_name: str, table_name: str, timestamp: str, dry_run: bool = True) -> Dict[str, Any]:
        """Revert a table to its state at a specific timestamp.

        This operation:
        1. Queries the history to find the state at the timestamp
        2. Clears the current table
        3. Restores rows that existed at that timestamp

        WARNING: This is a destructive operation. Use dry_run=True to preview changes first.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table
            timestamp: ISO timestamp to revert to
            dry_run: If True, only show what would be reverted without making changes (default: True)

        Returns:
            Dict with revert status and details
        """
        history_table = await self._get_history_table(schema_name, table_name)
        if not history_table:
            raise ValueError(f"Table {schema_name}.{table_name} is not temporally versioned")

        qualified_table = f"{schema_name}.{table_name}"

        # Get the base table columns
        columns = await self._get_table_columns(schema_name, table_name)
        column_list = ", ".join(columns)

        # Get current row count
        current_count_query = await self.sql_driver.execute_query(f"SELECT COUNT(*) as count FROM {qualified_table}")
        current_count_result = current_count_query[0].cells if current_count_query else None
        current_count = current_count_result["count"] if current_count_result else 0

        # Query what the table looked like at the timestamp
        historical_state = await self.query_at_timestamp(schema_name, table_name, timestamp, limit=999999)
        target_count = historical_state["row_count"]

        result = {
            "schema_name": schema_name,
            "table_name": table_name,
            "revert_to_timestamp": timestamp,
            "current_row_count": current_count,
            "target_row_count": target_count,
            "rows_to_delete": current_count,
            "rows_to_insert": target_count,
            "dry_run": dry_run,
        }

        if dry_run:
            result["message"] = "DRY RUN: No changes made. Set dry_run=False to execute revert."
            result["preview"] = historical_state["rows"][:10]  # Show first 10 rows as preview
            return result

        # Perform the revert
        # 1. Delete all current rows
        await self.sql_driver.execute_query(f"DELETE FROM {qualified_table}")

        # 2. Insert historical rows
        if historical_state["rows"]:
            # Build INSERT statement with placeholders
            placeholders = ", ".join(["%s"] * len(columns))
            insert_query = f"INSERT INTO {qualified_table} ({column_list}) VALUES ({placeholders})"

            # Insert each row
            for row in historical_state["rows"]:
                values = tuple(row[col] for col in columns)
                await self.sql_driver.execute_query(insert_query, values)

        result["message"] = f"Successfully reverted {qualified_table} to state at {timestamp}"
        result["reverted"] = True

        return result

    async def compare_timestamps(self, schema_name: str, table_name: str, timestamp1: str, timestamp2: str, limit: int = 100) -> Dict[str, Any]:
        """Compare table data between two timestamps.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table
            timestamp1: First timestamp (earlier)
            timestamp2: Second timestamp (later)
            limit: Maximum number of differences to return (default: 100)

        Returns:
            Dict with comparison results showing added, modified, and deleted rows
        """
        history_table = await self._get_history_table(schema_name, table_name)
        if not history_table:
            raise ValueError(f"Table {schema_name}.{table_name} is not temporally versioned")

        # Get data at both timestamps
        state1 = await self.query_at_timestamp(schema_name, table_name, timestamp1, limit=999999)
        state2 = await self.query_at_timestamp(schema_name, table_name, timestamp2, limit=999999)

        # Get the primary key column (assume first column for now)
        columns = await self._get_table_columns(schema_name, table_name)
        pk_column = columns[0] if columns else None

        if not pk_column:
            raise ValueError("Cannot compare: no columns found in table")

        # Build dictionaries keyed by primary key
        rows1 = {row[pk_column]: row for row in state1["rows"]}
        rows2 = {row[pk_column]: row for row in state2["rows"]}

        # Find differences
        added = []
        deleted = []
        modified = []

        # Find added and modified rows
        for pk, row2 in rows2.items():
            if pk not in rows1:
                added.append(row2)
            elif rows1[pk] != row2:
                modified.append({"before": rows1[pk], "after": row2})

        # Find deleted rows
        for pk, row1 in rows1.items():
            if pk not in rows2:
                deleted.append(row1)

        # Apply limit to each category
        added = added[:limit]
        deleted = deleted[:limit]
        modified = modified[:limit]

        return {
            "schema_name": schema_name,
            "table_name": table_name,
            "timestamp1": timestamp1,
            "timestamp2": timestamp2,
            "summary": {
                "rows_added": len(added),
                "rows_deleted": len(deleted),
                "rows_modified": len(modified),
            },
            "added_rows": added,
            "deleted_rows": deleted,
            "modified_rows": modified,
        }

    async def get_row_history(
        self, schema_name: str, table_name: str, primary_key_column: str, primary_key_value: Any, limit: int = 100
    ) -> Dict[str, Any]:
        """Get the complete history of changes for a specific row.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table
            primary_key_column: Name of the primary key column
            primary_key_value: Value of the primary key to track
            limit: Maximum number of changes to return (default: 100)

        Returns:
            Dict with the row's complete change history
        """
        history_table = await self._get_history_table(schema_name, table_name)
        if not history_table:
            raise ValueError(f"Table {schema_name}.{table_name} is not temporally versioned")

        # Get all changes for this specific row
        columns = await self._get_table_columns(schema_name, table_name)
        column_list = ", ".join(columns)

        query = f"""
        SELECT
            temporal_id,
            temporal_operation,
            temporal_valid_from::text as temporal_valid_from,
            temporal_tx_id,
            {column_list}
        FROM {history_table}
        WHERE {primary_key_column} = %s
        ORDER BY temporal_valid_from DESC, temporal_id DESC
        LIMIT %s
        """

        results_query = await self.sql_driver.execute_query(query, [primary_key_value, limit])
        results = [row.cells for row in results_query] if results_query else []

        return {
            "schema_name": schema_name,
            "table_name": table_name,
            "primary_key_column": primary_key_column,
            "primary_key_value": primary_key_value,
            "change_count": len(results),
            "history": results,
        }
