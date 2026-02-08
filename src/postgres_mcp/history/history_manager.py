"""History tracking manager for PostgreSQL tables.

This module provides functionality to enable and disable history tracking on PostgreSQL tables.
It creates history tables and triggers to automatically track all changes (INSERT, UPDATE, DELETE).
"""

import logging
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class TrackedTable:
    """Information about a table with history tracking enabled."""

    schema_name: str
    table_name: str
    history_table_name: str
    enabled: bool
    created_at: str | None = None


class HistoryManager:
    """Manages history tracking for PostgreSQL tables."""

    def __init__(self, sql_driver: Any):
        """Initialize the history manager.

        Args:
            sql_driver: SqlDriver instance for database operations
        """
        self.sql_driver = sql_driver

    async def _ensure_history_schema(self) -> None:
        """Ensure the history tracking schema and metadata table exist."""
        # Create schema for history tracking infrastructure
        await self.sql_driver.execute_query(
            """
            CREATE SCHEMA IF NOT EXISTS history_tracking
            """
        )

        # Create metadata table to track which tables have history tracking enabled
        await self.sql_driver.execute_query(
            """
            CREATE TABLE IF NOT EXISTS history_tracking.tracked_tables (
                id SERIAL PRIMARY KEY,
                schema_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                history_table_name TEXT NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (schema_name, table_name)
            )
            """
        )

    async def enable_tracking(self, schema_name: str, table_name: str, history_table_suffix: str = "_history") -> Dict[str, Any]:
        """Enable history tracking for a table.

        This creates a history table and triggers to automatically track all changes.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table to track
            history_table_suffix: Suffix for the history table name (default: "_history")

        Returns:
            Dict with status and details about the created history table
        """
        await self._ensure_history_schema()

        history_table_name = f"{table_name}{history_table_suffix}"
        qualified_table = f"{schema_name}.{table_name}"
        qualified_history = f"history_tracking.{history_table_name}"

        # Check if history tracking is already enabled
        existing_results = await self.sql_driver.execute_query(
            """
            SELECT * FROM history_tracking.tracked_tables
            WHERE schema_name = %s AND table_name = %s
            """,
            [schema_name, table_name],
        )
        existing = existing_results[0].cells if existing_results else None

        if existing and existing.get("enabled"):
            return {
                "status": "already_enabled",
                "message": f"History tracking is already enabled for {qualified_table}",
                "history_table": qualified_history,
            }

        # Get the table structure
        columns_query_result = await self.sql_driver.execute_query(
            """
            SELECT column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            [schema_name, table_name],
        )
        columns_result = [row.cells for row in columns_query_result] if columns_query_result else []

        if not columns_result:
            raise ValueError(f"Table {qualified_table} not found")

        # Build column definitions for history table
        column_defs = []
        column_names = []
        for col in columns_result:
            col_name = col["column_name"]
            col_type = col["data_type"]
            column_names.append(col_name)

            # Handle different data types
            if col_type == "character varying" and col["character_maximum_length"]:
                col_def = f"{col_name} VARCHAR({col['character_maximum_length']})"
            elif col_type == "numeric" and col["numeric_precision"]:
                if col["numeric_scale"]:
                    col_def = f"{col_name} NUMERIC({col['numeric_precision']}, {col['numeric_scale']})"
                else:
                    col_def = f"{col_name} NUMERIC({col['numeric_precision']})"
            else:
                col_def = f"{col_name} {col_type.upper()}"

            column_defs.append(col_def)

        # Add history tracking metadata columns
        column_defs.extend(
            [
                "history_id BIGSERIAL PRIMARY KEY",
                "change_operation VARCHAR(10) NOT NULL",  # 'INSERT', 'UPDATE', 'DELETE'
                "changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
                "valid_until TIMESTAMP",
                "transaction_id BIGINT NOT NULL DEFAULT txid_current()",
            ]
        )

        # Create history table
        create_history_sql = f"""
        CREATE TABLE IF NOT EXISTS {qualified_history} (
            {", ".join(column_defs)}
        )
        """
        await self.sql_driver.execute_query(create_history_sql)

        # Create index on history tracking columns for efficient querying
        await self.sql_driver.execute_query(
            f"""
            CREATE INDEX IF NOT EXISTS {history_table_name}_history_idx
            ON {qualified_history} (changed_at, valid_until)
            """
        )

        # Create trigger function to capture changes
        trigger_function_name = f"history_tracking.{table_name}_history_trigger"
        column_list = ", ".join(column_names)

        await self.sql_driver.execute_query(
            f"""
            CREATE OR REPLACE FUNCTION {trigger_function_name}()
            RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'DELETE') THEN
                    INSERT INTO {qualified_history} ({column_list}, change_operation, changed_at)
                    VALUES (OLD.*, 'DELETE', CURRENT_TIMESTAMP);
                    RETURN OLD;
                ELSIF (TG_OP = 'UPDATE') THEN
                    INSERT INTO {qualified_history} ({column_list}, change_operation, changed_at)
                    VALUES (OLD.*, 'UPDATE', CURRENT_TIMESTAMP);
                    RETURN NEW;
                ELSIF (TG_OP = 'INSERT') THEN
                    INSERT INTO {qualified_history} ({column_list}, change_operation, changed_at)
                    VALUES (NEW.*, 'INSERT', CURRENT_TIMESTAMP);
                    RETURN NEW;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """
        )

        # Create triggers for INSERT, UPDATE, DELETE
        trigger_name = f"{table_name}_history_trigger"
        await self.sql_driver.execute_query(f"DROP TRIGGER IF EXISTS {trigger_name} ON {qualified_table}")

        await self.sql_driver.execute_query(
            f"""
            CREATE TRIGGER {trigger_name}
            AFTER INSERT OR UPDATE OR DELETE ON {qualified_table}
            FOR EACH ROW EXECUTE FUNCTION {trigger_function_name}()
            """
        )

        # Register in metadata table
        await self.sql_driver.execute_query(
            """
            INSERT INTO history_tracking.tracked_tables (schema_name, table_name, history_table_name, enabled)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET enabled = TRUE, history_table_name = EXCLUDED.history_table_name
            """,
            (schema_name, table_name, history_table_name),
        )

        return {
            "status": "enabled",
            "message": f"History tracking enabled for {qualified_table}",
            "history_table": qualified_history,
            "trigger_name": trigger_name,
            "columns_tracked": len(column_names),
        }

    async def disable_versioning(self, schema_name: str, table_name: str, drop_history: bool = False) -> Dict[str, Any]:
        """Disable history tracking for a table.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table
            drop_history: If True, also drop the history table (default: False)

        Returns:
            Dict with status and details
        """
        qualified_table = f"{schema_name}.{table_name}"

        # Get versioning info
        version_info_result = await self.sql_driver.execute_query(
            """
            SELECT * FROM history_tracking.tracked_tables
            WHERE schema_name = %s AND table_name = %s
            """,
            [schema_name, table_name],
        )
        version_info = version_info_result[0].cells if version_info_result else None

        if not version_info:
            return {
                "status": "not_tracked",
                "message": f"History tracking is not enabled for {qualified_table}",
            }

        history_table_name = version_info["history_table_name"]
        qualified_history = f"history_tracking.{history_table_name}"

        # Drop trigger
        trigger_name = f"{table_name}_history_trigger"
        await self.sql_driver.execute_query(f"DROP TRIGGER IF EXISTS {trigger_name} ON {qualified_table}")

        # Drop trigger function
        trigger_function_name = f"history_tracking.{table_name}_history_trigger"
        await self.sql_driver.execute_query(f"DROP FUNCTION IF EXISTS {trigger_function_name}()")

        result: Dict[str, Any] = {
            "status": "disabled",
            "message": f"History tracking disabled for {qualified_table}",
            "history_table": qualified_history,
        }

        if drop_history:
            # Drop history table
            await self.sql_driver.execute_query(f"DROP TABLE IF EXISTS {qualified_history}")
            result["history_dropped"] = True
            result["message"] += " (history table dropped)"

            # Remove from metadata
            await self.sql_driver.execute_query(
                """
                DELETE FROM history_tracking.tracked_tables
                WHERE schema_name = %s AND table_name = %s
                """,
                (schema_name, table_name),
            )
        else:
            # Just mark as disabled
            await self.sql_driver.execute_query(
                """
                UPDATE history_tracking.tracked_tables
                SET enabled = FALSE
                WHERE schema_name = %s AND table_name = %s
                """,
                (schema_name, table_name),
            )
            result["history_preserved"] = True
            result["message"] += " (history table preserved)"

        return result

    async def list_tracked_tables(self) -> List[TrackedTable]:
        """List all tables with history tracking enabled.

        Returns:
            List of TrackedTable objects
        """
        try:
            await self._ensure_history_schema()
        except Exception:
            # If schema doesn't exist, no tables have history tracking
            return []

        results_query = await self.sql_driver.execute_query(
            """
            SELECT schema_name, table_name, history_table_name, enabled,
                   created_at::text as created_at
            FROM history_tracking.tracked_tables
            ORDER BY schema_name, table_name
            """
        )
        results = [row.cells for row in results_query] if results_query else []

        return [
            TrackedTable(
                schema_name=row["schema_name"],
                table_name=row["table_name"],
                history_table_name=row["history_table_name"],
                enabled=row["enabled"],
                created_at=row.get("created_at"),
            )
            for row in results
        ]

    async def get_versioning_status(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get detailed versioning status for a specific table.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table

        Returns:
            Dict with versioning status and statistics
        """
        try:
            await self._ensure_history_schema()
        except Exception:
            return {"tracked": False, "message": "History tracking not initialized"}

        version_info_result = await self.sql_driver.execute_query(
            """
            SELECT * FROM history_tracking.tracked_tables
            WHERE schema_name = %s AND table_name = %s
            """,
            [schema_name, table_name],
        )
        version_info = version_info_result[0].cells if version_info_result else None

        if not version_info:
            return {
                "tracked": False,
                "message": f"Table {schema_name}.{table_name} does not have history tracking enabled",
            }

        qualified_history = f"history_tracking.{version_info['history_table_name']}"

        # Get statistics about the history table
        stats_result = await self.sql_driver.execute_query(
            f"""
            SELECT
                COUNT(*) as total_changes,
                COUNT(DISTINCT transaction_id) as total_transactions,
                MIN(changed_at)::text as first_change,
                MAX(changed_at)::text as last_change,
                SUM(CASE WHEN change_operation = 'INSERT' THEN 1 ELSE 0 END) as inserts,
                SUM(CASE WHEN change_operation = 'UPDATE' THEN 1 ELSE 0 END) as updates,
                SUM(CASE WHEN change_operation = 'DELETE' THEN 1 ELSE 0 END) as deletes
            FROM {qualified_history}
            """
        )
        stats = stats_result[0].cells if stats_result else None

        return {
            "tracked": True,
            "enabled": version_info["enabled"],
            "schema_name": version_info["schema_name"],
            "table_name": version_info["table_name"],
            "history_table": qualified_history,
            "created_at": version_info.get("created_at"),
            "statistics": stats,
        }
