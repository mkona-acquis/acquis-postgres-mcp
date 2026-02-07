"""Temporal table versioning manager for PostgreSQL.

This module provides functionality to enable and disable temporal versioning on PostgreSQL tables.
It creates history tables and triggers to automatically track all changes (INSERT, UPDATE, DELETE).
"""

import logging
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class TemporalTable:
    """Information about a temporally versioned table."""

    schema_name: str
    table_name: str
    history_table_name: str
    enabled: bool
    created_at: str | None = None


class TemporalManager:
    """Manages temporal versioning for PostgreSQL tables."""

    def __init__(self, sql_driver: Any):
        """Initialize the temporal manager.

        Args:
            sql_driver: SqlDriver instance for database operations
        """
        self.sql_driver = sql_driver

    async def _ensure_temporal_schema(self) -> None:
        """Ensure the temporal versioning schema and metadata table exist."""
        # Create schema for temporal infrastructure
        await self.sql_driver.execute(
            """
            CREATE SCHEMA IF NOT EXISTS temporal_versioning
            """
        )

        # Create metadata table to track which tables have versioning enabled
        await self.sql_driver.execute(
            """
            CREATE TABLE IF NOT EXISTS temporal_versioning.versioned_tables (
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

    async def enable_versioning(self, schema_name: str, table_name: str, history_table_suffix: str = "_history") -> Dict[str, Any]:
        """Enable temporal versioning for a table.

        This creates a history table and triggers to automatically track all changes.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table to version
            history_table_suffix: Suffix for the history table name (default: "_history")

        Returns:
            Dict with status and details about the created history table
        """
        await self._ensure_temporal_schema()

        history_table_name = f"{table_name}{history_table_suffix}"
        qualified_table = f"{schema_name}.{table_name}"
        qualified_history = f"temporal_versioning.{history_table_name}"

        # Check if versioning is already enabled
        existing = await self.sql_driver.fetchone(
            """
            SELECT * FROM temporal_versioning.versioned_tables
            WHERE schema_name = %s AND table_name = %s
            """,
            (schema_name, table_name),
        )

        if existing and existing.get("enabled"):
            return {
                "status": "already_enabled",
                "message": f"Temporal versioning is already enabled for {qualified_table}",
                "history_table": qualified_history,
            }

        # Get the table structure
        columns_result = await self.sql_driver.fetchall(
            """
            SELECT column_name, data_type, character_maximum_length,
                   numeric_precision, numeric_scale, is_nullable
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema_name, table_name),
        )

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

        # Add temporal metadata columns
        column_defs.extend(
            [
                "temporal_id BIGSERIAL PRIMARY KEY",
                "temporal_operation VARCHAR(10) NOT NULL",  # 'INSERT', 'UPDATE', 'DELETE'
                "temporal_valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP",
                "temporal_valid_to TIMESTAMP",
                "temporal_tx_id BIGINT NOT NULL DEFAULT txid_current()",
            ]
        )

        # Create history table
        create_history_sql = f"""
        CREATE TABLE IF NOT EXISTS {qualified_history} (
            {", ".join(column_defs)}
        )
        """
        await self.sql_driver.execute(create_history_sql)

        # Create index on temporal columns for efficient querying
        await self.sql_driver.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {history_table_name}_temporal_idx
            ON {qualified_history} (temporal_valid_from, temporal_valid_to)
            """
        )

        # Create trigger function to capture changes
        trigger_function_name = f"temporal_versioning.{table_name}_history_trigger"
        column_list = ", ".join(column_names)

        await self.sql_driver.execute(
            f"""
            CREATE OR REPLACE FUNCTION {trigger_function_name}()
            RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'DELETE') THEN
                    INSERT INTO {qualified_history} ({column_list}, temporal_operation, temporal_valid_from)
                    VALUES (OLD.*, 'DELETE', CURRENT_TIMESTAMP);
                    RETURN OLD;
                ELSIF (TG_OP = 'UPDATE') THEN
                    INSERT INTO {qualified_history} ({column_list}, temporal_operation, temporal_valid_from)
                    VALUES (OLD.*, 'UPDATE', CURRENT_TIMESTAMP);
                    RETURN NEW;
                ELSIF (TG_OP = 'INSERT') THEN
                    INSERT INTO {qualified_history} ({column_list}, temporal_operation, temporal_valid_from)
                    VALUES (NEW.*, 'INSERT', CURRENT_TIMESTAMP);
                    RETURN NEW;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """
        )

        # Create triggers for INSERT, UPDATE, DELETE
        trigger_name = f"{table_name}_temporal_trigger"
        await self.sql_driver.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {qualified_table}")

        await self.sql_driver.execute(
            f"""
            CREATE TRIGGER {trigger_name}
            AFTER INSERT OR UPDATE OR DELETE ON {qualified_table}
            FOR EACH ROW EXECUTE FUNCTION {trigger_function_name}()
            """
        )

        # Register in metadata table
        await self.sql_driver.execute(
            """
            INSERT INTO temporal_versioning.versioned_tables (schema_name, table_name, history_table_name, enabled)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (schema_name, table_name)
            DO UPDATE SET enabled = TRUE, history_table_name = EXCLUDED.history_table_name
            """,
            (schema_name, table_name, history_table_name),
        )

        return {
            "status": "enabled",
            "message": f"Temporal versioning enabled for {qualified_table}",
            "history_table": qualified_history,
            "trigger_name": trigger_name,
            "columns_tracked": len(column_names),
        }

    async def disable_versioning(self, schema_name: str, table_name: str, drop_history: bool = False) -> Dict[str, Any]:
        """Disable temporal versioning for a table.

        Args:
            schema_name: Schema containing the table
            table_name: Name of the table
            drop_history: If True, also drop the history table (default: False)

        Returns:
            Dict with status and details
        """
        qualified_table = f"{schema_name}.{table_name}"

        # Get versioning info
        version_info = await self.sql_driver.fetchone(
            """
            SELECT * FROM temporal_versioning.versioned_tables
            WHERE schema_name = %s AND table_name = %s
            """,
            (schema_name, table_name),
        )

        if not version_info:
            return {
                "status": "not_versioned",
                "message": f"Temporal versioning is not enabled for {qualified_table}",
            }

        history_table_name = version_info["history_table_name"]
        qualified_history = f"temporal_versioning.{history_table_name}"

        # Drop trigger
        trigger_name = f"{table_name}_temporal_trigger"
        await self.sql_driver.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {qualified_table}")

        # Drop trigger function
        trigger_function_name = f"temporal_versioning.{table_name}_history_trigger"
        await self.sql_driver.execute(f"DROP FUNCTION IF EXISTS {trigger_function_name}()")

        result: Dict[str, Any] = {
            "status": "disabled",
            "message": f"Temporal versioning disabled for {qualified_table}",
            "history_table": qualified_history,
        }

        if drop_history:
            # Drop history table
            await self.sql_driver.execute(f"DROP TABLE IF EXISTS {qualified_history}")
            result["history_dropped"] = True
            result["message"] += " (history table dropped)"

            # Remove from metadata
            await self.sql_driver.execute(
                """
                DELETE FROM temporal_versioning.versioned_tables
                WHERE schema_name = %s AND table_name = %s
                """,
                (schema_name, table_name),
            )
        else:
            # Just mark as disabled
            await self.sql_driver.execute(
                """
                UPDATE temporal_versioning.versioned_tables
                SET enabled = FALSE
                WHERE schema_name = %s AND table_name = %s
                """,
                (schema_name, table_name),
            )
            result["history_preserved"] = True
            result["message"] += " (history table preserved)"

        return result

    async def list_versioned_tables(self) -> List[TemporalTable]:
        """List all tables with temporal versioning.

        Returns:
            List of TemporalTable objects
        """
        try:
            await self._ensure_temporal_schema()
        except Exception:
            # If schema doesn't exist, no tables are versioned
            return []

        results = await self.sql_driver.fetchall(
            """
            SELECT schema_name, table_name, history_table_name, enabled,
                   created_at::text as created_at
            FROM temporal_versioning.versioned_tables
            ORDER BY schema_name, table_name
            """
        )

        return [
            TemporalTable(
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
            await self._ensure_temporal_schema()
        except Exception:
            return {"versioned": False, "message": "Temporal versioning not initialized"}

        version_info = await self.sql_driver.fetchone(
            """
            SELECT * FROM temporal_versioning.versioned_tables
            WHERE schema_name = %s AND table_name = %s
            """,
            (schema_name, table_name),
        )

        if not version_info:
            return {
                "versioned": False,
                "message": f"Table {schema_name}.{table_name} is not versioned",
            }

        qualified_history = f"temporal_versioning.{version_info['history_table_name']}"

        # Get statistics about the history table
        stats = await self.sql_driver.fetchone(
            f"""
            SELECT
                COUNT(*) as total_changes,
                COUNT(DISTINCT temporal_tx_id) as total_transactions,
                MIN(temporal_valid_from)::text as first_change,
                MAX(temporal_valid_from)::text as last_change,
                SUM(CASE WHEN temporal_operation = 'INSERT' THEN 1 ELSE 0 END) as inserts,
                SUM(CASE WHEN temporal_operation = 'UPDATE' THEN 1 ELSE 0 END) as updates,
                SUM(CASE WHEN temporal_operation = 'DELETE' THEN 1 ELSE 0 END) as deletes
            FROM {qualified_history}
            """
        )

        return {
            "versioned": True,
            "enabled": version_info["enabled"],
            "schema_name": version_info["schema_name"],
            "table_name": version_info["table_name"],
            "history_table": qualified_history,
            "created_at": version_info.get("created_at"),
            "statistics": stats,
        }
