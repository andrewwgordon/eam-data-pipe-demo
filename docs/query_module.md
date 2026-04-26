# Iceberg Query Module

## Overview

The Iceberg Query Module provides a unified query interface for analytical exploration across all data layers (Silver, Silver-S5000F, and Gold) using Polars SQL. This module enables users to query Iceberg tables with automatic partition filtering and cross-layer joins.

## Architecture

```
┌─────────────────────────────────────────────┐
│            Query Module                     │
│  transforms/polars/query/                   │
├─────────────────────────────────────────────┤
│  iceberg_query.py      - Main query function│
│  examples/             - Example queries    │
│    ├── asset_availability_query.py          │
│    ├── maintenance_history_query.py         │
│    ├── s5000f_compliance_query.py          │
│    └── cross_layer_comparison.py           │
└─────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│          Polars SQL Context                 │
│  - Table registration                       │
│  - SQL execution                            │
│  - Result collection                        │
└─────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│          Iceberg Catalog                    │
│  - Table loading                            │
│  - Partition filtering                      │
│  - Schema inference                         │
└─────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│          Data Layers                        │
│  - Silver (Application)                     │
│  - Silver-S5000F (Standardised)             │
│  - Gold (Analytics)                         │
└─────────────────────────────────────────────┘
```

## Key Features

### 1. Unified Query Interface
- Single function `query_iceberg()` for all query types
- Support for both table-based and custom SQL queries
- Automatic partition filtering by date

### 2. Cross-Layer Joins
- Join tables across different data layers
- Compare application vs S5000F representations
- Unified analytics across all layers

### 3. Polars SQL Integration
- Full Polars SQL syntax support
- Lazy execution with automatic optimization
- Seamless integration with existing Polars workflows

### 4. Automatic Table Discovery
- `get_available_tables()` lists all tables in each namespace
- Dynamic table registration for SQL queries
- Error handling for missing tables

## Usage Examples

### Basic Table Query
```python
from transforms.polars.query import query_iceberg

# Query a single table with limit
df = query_iceberg(
    table_name="silver.asset",
    date_filter="2024-01-01",
    limit=10
)
```

### Custom SQL Query
```python
from transforms.polars.query import query_iceberg

sql = """
SELECT 
    a.id as asset_id,
    a.name as asset_name,
    a.asset_type,
    a.status,
    ga.availability_percentage
FROM silver_asset a
LEFT JOIN gold_asset_availability ga ON a.id = ga.asset_id
WHERE ga.date = '2024-01-01'
ORDER BY ga.availability_percentage DESC
LIMIT 5
"""

df = query_iceberg(sql=sql, date_filter="2024-01-01")
```

### Table Discovery
```python
from transforms.polars.query import get_available_tables

tables = get_available_tables()
print(f"Silver tables: {tables['silver']}")
print(f"Silver-S5000F tables: {tables['silver_s5000f']}")
print(f"Gold tables: {tables['gold']}")
```

### Example Queries
```python
from transforms.polars.query.examples import (
    run_asset_availability_queries,
    run_maintenance_history_queries,
    run_s5000f_compliance_queries,
    run_cross_layer_comparison_queries
)

# Run all example queries
run_asset_availability_queries()
run_maintenance_history_queries()
run_s5000f_compliance_queries()
run_cross_layer_comparison_queries()
```

## API Reference

### `query_iceberg()`
```python
def query_iceberg(
    table_name: Optional[str] = None,
    sql: Optional[str] = None,
    date_filter: Optional[Union[str, date, datetime]] = None,
    limit: Optional[int] = None,
    settings: Optional[Settings] = None,
) -> pl.DataFrame
```

**Parameters:**
- `table_name`: Fully qualified Iceberg table name (e.g., "silver.asset")
- `sql`: Custom SQL query string (if provided, `table_name` is ignored)
- `date_filter`: Date to filter partitions by (YYYY-MM-DD format)
- `limit`: Maximum number of rows to return
- `settings`: Application settings (optional)

**Returns:** Polars DataFrame with query results

### `get_available_tables()`
```python
def get_available_tables(
    settings: Optional[Settings] = None,
) -> dict[str, list[str]]
```

**Returns:** Dictionary mapping namespace to list of table names

### `example_queries()`
```python
def example_queries() -> dict[str, str]
```

**Returns:** Dictionary of example SQL queries for common patterns

## Example Query Patterns

### 1. Asset Availability Analysis
```sql
SELECT 
    a.id as asset_id,
    a.name as asset_name,
    a.asset_type,
    ga.availability_percentage,
    ga.downtime_hours,
    CASE 
        WHEN ga.availability_percentage >= 95 THEN 'EXCELLENT'
        WHEN ga.availability_percentage >= 90 THEN 'GOOD'
        WHEN ga.availability_percentage >= 80 THEN 'FAIR'
        ELSE 'POOR'
    END as health_status
FROM silver_asset a
LEFT JOIN gold_asset_availability ga ON a.id = ga.asset_id
WHERE ga.date = '2024-01-01'
ORDER BY ga.availability_percentage DESC
```

### 2. Maintenance History Tracking
```sql
SELECT 
    a.id as asset_id,
    a.name as asset_name,
    wo.id as work_order_id,
    wo.description,
    wo.status as wo_status,
    wo.created_at as wo_created,
    wo.completed_at as wo_completed,
    COUNT(ma.id) as action_count,
    SUM(ma.duration_minutes) as total_duration_minutes
FROM silver_asset a
JOIN silver_work_order wo ON a.id = wo.asset_id
LEFT JOIN silver_maintenance_action ma ON wo.id = ma.work_order_id
WHERE wo.status = 'COMPLETED'
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY wo.completed_at DESC
```

### 3. S5000F Compliance Check
```sql
SELECT 
    pi.product_instance_id,
    pi.serial_number,
    pi.manufacturer,
    mt.maintenance_task_id,
    mt.task_description,
    mt.planned_start_date,
    mt.planned_completion_date,
    me.actual_start_date,
    me.actual_completion_date,
    CASE 
        WHEN me.actual_completion_date IS NOT NULL THEN 'COMPLETED'
        WHEN me.actual_start_date IS NOT NULL THEN 'IN_PROGRESS'
        ELSE 'PLANNED'
    END as execution_status
FROM silver_s5000f_product_instance pi
JOIN silver_s5000f_maintenance_task mt ON pi.product_instance_id = mt.product_instance_id
LEFT JOIN silver_s5000f_maintenance_event me ON mt.maintenance_task_id = me.maintenance_task_id
ORDER BY mt.planned_start_date
```

### 4. Cross-Layer Comparison
```sql
SELECT 
    'Silver (Application)' as data_layer,
    a.id as entity_id,
    a.name as entity_name,
    a.asset_type,
    a.status
FROM silver_asset a

UNION ALL

SELECT 
    'Silver-S5000F (Standardised)' as data_layer,
    pi.product_instance_id as entity_id,
    pi.serial_number as entity_name,
    NULL as asset_type,
    pi.status
FROM silver_s5000f_product_instance pi

ORDER BY data_layer, entity_id
```

## Implementation Details

### Table Name Sanitization
- Iceberg table names (e.g., `silver.asset`) are sanitized to `silver_asset` for SQL context
- The module automatically converts between these formats
- Table extraction from SQL queries handles both formats

### Partition Filtering
- Automatically detects `event_date` or `date` partition columns
- Applies filter at query time for performance
- Supports string, date, and datetime filter formats

### Error Handling
- Graceful handling of missing tables
- Informative error messages for SQL syntax errors
- Logging of query execution details

## Integration with Existing Workflow

The query module integrates seamlessly with the existing EAM Data Pipe architecture:

1. **Data Generation**: EAM simulator creates CDC events
2. **CDC Processing**: Bronze → Silver transformations
3. **Semantic Mapping**: Silver → Silver-S5000F transformations  
4. **Analytics**: Gold table generation
5. **Query & Analysis**: Iceberg Query Module for exploration

## Running the Examples

```bash
# Run all example queries
python -m transforms.polars.query.examples.asset_availability_query
python -m transforms.polars.query.examples.maintenance_history_query
python -m transforms.polars.query.examples.s5000f_compliance_query
python -m transforms.polars.query.examples.cross_layer_comparison

# Or run them all from a script
python -c "
from transforms.polars.query.examples import *
run_asset_availability_queries()
run_maintenance_history_queries()
run_s5000f_compliance_queries()
run_cross_layer_comparison_queries()
"
```

## Best Practices

1. **Use Date Filtering**: Always specify `date_filter` for partition pruning
2. **Limit Results**: Use `limit` parameter for exploratory queries
3. **Check Available Tables**: Use `get_available_tables()` before complex queries
4. **Test with Examples**: Start with example queries and modify as needed
5. **Monitor Logs**: Check query execution logs for performance insights

## Limitations

1. **SQL Dialect**: Uses Polars SQL dialect (subset of standard SQL)
2. **Table Discovery**: Requires tables to exist in Iceberg catalog
3. **Partition Columns**: Only supports `event_date` and `date` partition columns
4. **Performance**: Complex joins may require optimization for large datasets

## Future Enhancements

1. **Query Caching**: Add result caching for repeated queries
2. **Query Optimization**: Automatic query optimization hints
3. **Additional Dialects**: Support for more SQL dialects
4. **Visualization Integration**: Integration with data visualization tools
5. **Query Templates**: Pre-defined query templates for common patterns