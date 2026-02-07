# Temporal Table Versioning

Temporal versioning provides automatic change tracking for PostgreSQL tables, enabling you to:
- Track all changes (INSERT, UPDATE, DELETE) automatically
- Query data as it existed at any point in time
- Revert tables to previous states
- Audit and analyze data transformations
- Build safety nets for data migrations

## Quick Start

### 1. Enable Versioning on a Table

```python
# Enable versioning for a table
enable_temporal_versioning(
    schema_name="public",
    table_name="customers",
    history_table_suffix="_history"  # Optional, default is "_history"
)
```

This creates:
- A history table (`temporal_versioning.customers_history`) to store all changes
- Triggers that automatically capture INSERT, UPDATE, and DELETE operations
- Metadata tracking in `temporal_versioning.versioned_tables`

### 2. Make Changes to Your Data

Once versioning is enabled, all changes are automatically tracked:

```sql
-- These operations are automatically captured in the history table
INSERT INTO customers (id, name, email) VALUES (1, 'Alice', 'alice@example.com');
UPDATE customers SET email = 'alice.new@example.com' WHERE id = 1;
DELETE FROM customers WHERE id = 1;
```

### 3. Query Historical Data

```python
# View data as it existed at a specific timestamp
query_temporal_data(
    schema_name="public",
    table_name="customers",
    timestamp="2024-01-15 10:30:00",
    limit=100
)

# Get change history for a time range
get_change_history(
    schema_name="public",
    table_name="customers",
    start_time="2024-01-15 00:00:00",
    end_time="2024-01-15 23:59:59",
    operation="UPDATE"  # Optional: filter by INSERT, UPDATE, or DELETE
)
```

### 4. Revert Data (with Safety)

```python
# ALWAYS preview first with dry_run=True
revert_table_data(
    schema_name="public",
    table_name="customers",
    timestamp="2024-01-15 10:00:00",
    dry_run=True  # Shows what would change without making changes
)

# After reviewing the preview, execute the revert
revert_table_data(
    schema_name="public",
    table_name="customers",
    timestamp="2024-01-15 10:00:00",
    dry_run=False  # Actually performs the revert
)
```

## Common Use Cases

### Data Migration Safety Net

```python
# Before starting a complex data transformation:
enable_temporal_versioning(schema_name="public", table_name="orders")

# Perform your migration/transformation
# ... run migration scripts ...

# If something goes wrong, revert to before the migration
revert_table_data(
    schema_name="public",
    table_name="orders",
    timestamp="2024-01-15 09:00:00",  # Timestamp before migration started
    dry_run=False
)
```

### Understanding Data Changes

```python
# Compare data between two points in time
compare_temporal_data(
    schema_name="public",
    table_name="products",
    timestamp1="2024-01-01 00:00:00",
    timestamp2="2024-01-31 23:59:59"
)
# Returns: added_rows, deleted_rows, modified_rows

# Track a specific row's history
get_row_history(
    schema_name="public",
    table_name="customers",
    primary_key_column="id",
    primary_key_value="12345"
)
# Returns: complete change history for customer ID 12345
```

### Audit Trail

```python
# Get all changes in the last 24 hours
get_change_history(
    schema_name="public",
    table_name="transactions",
    start_time="2024-01-15 00:00:00",
    end_time="2024-01-16 00:00:00",
    limit=1000
)
```

## Managing Versioned Tables

### List All Versioned Tables

```python
list_temporal_tables()
# Returns: list of all tables with versioning enabled/disabled
```

### Check Status of a Table

```python
get_temporal_status(
    schema_name="public",
    table_name="customers"
)
# Returns: versioning status, statistics (total changes, inserts, updates, deletes)
```

### Disable Versioning

```python
# Disable versioning but keep history for analysis
disable_temporal_versioning(
    schema_name="public",
    table_name="customers",
    drop_history=False  # Preserves history table
)

# Disable and remove all history
disable_temporal_versioning(
    schema_name="public",
    table_name="customers",
    drop_history=True  # Deletes history table
)
```

## How It Works

### History Table Structure

For a table `public.customers`, versioning creates `temporal_versioning.customers_history`:

```sql
CREATE TABLE temporal_versioning.customers_history (
    -- All columns from the original table
    id INT,
    name VARCHAR(100),
    email VARCHAR(255),
    -- Temporal metadata columns
    temporal_id BIGSERIAL PRIMARY KEY,
    temporal_operation VARCHAR(10) NOT NULL,  -- 'INSERT', 'UPDATE', 'DELETE'
    temporal_valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    temporal_valid_to TIMESTAMP,
    temporal_tx_id BIGINT NOT NULL DEFAULT txid_current()
);
```

### Automatic Capture with Triggers

Changes are captured via PostgreSQL triggers:
- **INSERT**: Captures new row data
- **UPDATE**: Captures old row data before update
- **DELETE**: Captures deleted row data

### Point-in-Time Reconstruction

When querying historical data at a timestamp:
1. Finds the most recent change before the timestamp for each row
2. Filters out rows that were deleted
3. Returns the reconstructed table state

## Best Practices

### 1. Always Use Dry Run First

```python
# WRONG: Reverting without preview
revert_table_data(..., dry_run=False)

# RIGHT: Always preview first
revert_table_data(..., dry_run=True)  # Review the preview
revert_table_data(..., dry_run=False)  # Then execute
```

### 2. Enable Versioning Before Risky Operations

Enable versioning before complex migrations, bulk updates, or data transformations.

### 3. Monitor History Table Size

History tables grow over time. Monitor size and consider:
- Archiving old history data
- Disabling versioning when no longer needed
- Using partitioning for large history tables

### 4. Use Specific Timestamps

```python
# GOOD: Specific timestamp
timestamp="2024-01-15 10:30:00"

# BETTER: Use database timestamps
# Get timestamp before operation: SELECT CURRENT_TIMESTAMP
# Then use that exact timestamp for reverting
```

### 5. Test in Development First

Always test temporal operations in development/staging environments before production.

## Limitations

- **Primary Key Requirement**: Point-in-time queries work best with tables that have a stable primary key
- **Performance**: History tables can grow large; query performance may degrade over time
- **Storage**: History tables consume additional disk space
- **NOT FOR**: Real-time high-throughput tables (consider partitioning or alternative approaches)

## Architecture Notes

- History tables are stored in the `temporal_versioning` schema
- Metadata is tracked in `temporal_versioning.versioned_tables`
- Uses PostgreSQL triggers for automatic change capture
- Compatible with both UNRESTRICTED and RESTRICTED access modes
- Revert operations require UNRESTRICTED mode

## Troubleshooting

### History Table Growing Too Large

```python
# Archive old history data before a certain date
# Execute directly: DELETE FROM temporal_versioning.customers_history
#                   WHERE temporal_valid_from < '2023-01-01'

# Or disable and re-enable versioning to start fresh
disable_temporal_versioning(..., drop_history=True)
enable_temporal_versioning(...)
```

### Query Performance Degradation

```sql
-- Add indexes on commonly queried columns
CREATE INDEX idx_temporal_valid_from
ON temporal_versioning.customers_history(temporal_valid_from);

-- For row history queries
CREATE INDEX idx_primary_key
ON temporal_versioning.customers_history(id);
```

## Examples from Common Scenarios

### ETL Pipeline Safety

```python
# 1. Before ETL starts
enable_temporal_versioning(schema_name="public", table_name="staging_table")
checkpoint_time = "2024-01-15 08:00:00"  # Record current time

# 2. Run ETL
# ... ETL process ...

# 3. If issues found
compare_temporal_data(
    schema_name="public",
    table_name="staging_table",
    timestamp1=checkpoint_time,
    timestamp2="2024-01-15 09:00:00"
)

# 4. Rollback if needed
revert_table_data(
    schema_name="public",
    table_name="staging_table",
    timestamp=checkpoint_time,
    dry_run=False
)
```

### Data Quality Monitoring

```python
# Track changes to critical tables
get_change_history(
    schema_name="public",
    table_name="financial_transactions",
    start_time="2024-01-15 00:00:00",
    operation="DELETE"  # Alert on any deletions
)
```
