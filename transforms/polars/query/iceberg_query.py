"""Iceberg Query Function — Unified query interface for analytical exploration.

Provides a reusable Polars SQL query function for querying Silver, Silver-S5000F,
and Gold Iceberg tables with automatic partition filtering and cross-layer joins.

Usage:
    from transforms.polars.query.iceberg_query import query_iceberg
    
    # Query a single table
    df = query_iceberg("silver.asset", date_filter="2024-01-01")
    
    # Execute a custom SQL query
    df = query_iceberg(
        sql=\"\"\"
        SELECT a.id, a.name, wo.status, wo.priority
        FROM silver.asset a
        JOIN silver.work_order wo ON a.id = wo.asset_id
        WHERE wo.status = 'OPEN'
        \"\"\",
        date_filter="2024-01-01"
    )
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional, Union

import polars as pl
from pyiceberg.table import Table

from config.settings import Settings
from transforms.polars.app.iceberg_io import get_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def query_iceberg(
    table_name: Optional[str] = None,
    sql: Optional[str] = None,
    date_filter: Optional[Union[str, date, datetime]] = None,
    limit: Optional[int] = None,
    settings: Optional[Settings] = None,
) -> pl.DataFrame:
    """Query Iceberg tables using Polars SQL.
    
    Provides a unified interface for querying Silver, Silver-S5000F, and Gold tables.
    Supports automatic partition filtering by date and cross-layer joins.
    
    Args:
        table_name: Fully qualified Iceberg table name (e.g., "silver.asset", 
                   "silver_s5000f.product_instance", "gold.asset_availability")
        sql: Custom SQL query string. If provided, table_name is ignored.
        date_filter: Date to filter partitions by. Can be string (YYYY-MM-DD), 
                    date, or datetime object.
        limit: Maximum number of rows to return.
        settings: Application settings. If None, loads default settings.
        
    Returns:
        Polars DataFrame with query results.
        
    Raises:
        ValueError: If neither table_name nor sql is provided.
        Exception: If table doesn't exist or query fails.
    """
    if not table_name and not sql:
        raise ValueError("Either table_name or sql must be provided")
    
    settings = settings or Settings()
    catalog = get_catalog(settings)
    
    # Register Iceberg tables with Polars SQL context
    ctx = pl.SQLContext()
    
    if sql:
        # Custom SQL query - need to register all relevant tables
        logger.info(f"Executing custom SQL query")
        
        # Parse SQL to identify tables (simplified approach)
        # For production, consider using a proper SQL parser
        tables_to_load = _extract_tables_from_sql(sql)
        
        for table_ref in tables_to_load:
            try:
                table = catalog.load_table(table_ref)
                lazy_df = _read_table_with_partition_filter(table, date_filter)
                # Convert to DataFrame for SQL context
                df = lazy_df.collect()
                # Register with sanitized name (replace dots with underscores)
                table_name_sanitized = table_ref.replace(".", "_")
                ctx.register(table_name_sanitized, df)
                logger.debug(f"Registered table: {table_ref} as {table_name_sanitized}")
            except Exception as e:
                logger.warning(f"Could not load table {table_ref}: {e}")
                # Continue with other tables - might be referenced in JOIN but not needed
                pass
        
        # Execute the query
        query_result = ctx.execute(sql)
        
    else:
        # Single table query
        logger.info(f"Querying table: {table_name}")
        
        try:
            table = catalog.load_table(table_name)
            lazy_df = _read_table_with_partition_filter(table, date_filter)
            df = lazy_df.collect()
            
            # Register for potential subqueries
            table_name_sanitized = table_name.replace(".", "_")
            ctx.register(table_name_sanitized, df)
            
            # Build simple SELECT query
            select_sql = f"SELECT * FROM {table_name_sanitized}"
            if limit:
                select_sql += f" LIMIT {limit}"
            
            query_result = ctx.execute(select_sql)
            
        except Exception as e:
            logger.error(f"Failed to query table {table_name}: {e}")
            raise
    
    # Convert LazyFrame to DataFrame
    query_result = query_result.collect()
    
    # Apply limit if specified (for custom SQL queries)
    if limit and len(query_result) > limit:
        query_result = query_result.head(limit)
    
    logger.info(f"Query returned {len(query_result)} rows")
    return query_result


def _read_table_with_partition_filter(table: Table, date_filter: Optional[Union[str, date, datetime]] = None) -> pl.LazyFrame:
    """Read Iceberg table with optional date partition filtering.
    
    Args:
        table: PyIceberg Table object.
        date_filter: Date to filter partitions by.
        
    Returns:
        Polars LazyFrame with filtered data.
    """
    # Use the existing read_iceberg_table function from iceberg_io
    from transforms.polars.app.iceberg_io import read_iceberg_table
    
    # Get the lazy frame
    lazy_df = read_iceberg_table(table)
    
    # Apply date partition filter if specified
    if date_filter and lazy_df.collect_schema().names():
        # Convert date_filter to string format
        if isinstance(date_filter, (date, datetime)):
            date_str = date_filter.strftime("%Y-%m-%d")
        else:
            date_str = str(date_filter)
        
        # Check if table has event_date or date column
        columns = lazy_df.collect_schema().names()
        
        if "event_date" in columns:
            lazy_df = lazy_df.filter(pl.col("event_date") == date_str)
            logger.debug(f"Applied partition filter: event_date = {date_str}")
        elif "date" in columns:
            lazy_df = lazy_df.filter(pl.col("date") == date_str)
            logger.debug(f"Applied partition filter: date = {date_str}")
    
    return lazy_df


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extract table references from SQL query (simplified).
    
    Args:
        sql: SQL query string.
        
    Returns:
        List of table names referenced in the query.
    """
    # Remove comments and split into words
    lines = sql.split('\n')
    clean_lines = []
    for line in lines:
        if '--' in line:
            line = line.split('--')[0]
        clean_lines.append(line)
    
    clean_sql = ' '.join(clean_lines)
    
    # Simple regex to find table names after FROM and JOIN
    import re
    
    # Look for patterns with underscores (sanitized names)
    # First, find all table references with underscores
    table_patterns = [
        r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*(?:_[a-zA-Z_][a-zA-Z0-9_]*)+)',
        r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*(?:_[a-zA-Z_][a-zA-Z0-9_]*)+)'
    ]
    
    tables = []
    for pattern in table_patterns:
        matches = re.findall(pattern, clean_sql, re.IGNORECASE)
        tables.extend(matches)
    
    # Convert sanitized names back to original Iceberg names
    # e.g., silver_asset -> silver.asset
    # Known namespaces that might appear in SQL
    known_namespaces = ["silver", "silver_s5000f", "gold"]
    
    original_tables = []
    for table in tables:
        # Check if table starts with a known namespace
        for namespace in known_namespaces:
            if table.startswith(namespace + "_"):
                # Extract table name after namespace and underscore
                table_name = table[len(namespace) + 1:]  # +1 for the underscore
                original_tables.append(f"{namespace}.{table_name}")
                break
        else:
            # If no known namespace matched, use simple split
            if '_' in table:
                parts = table.split('_')
                if len(parts) >= 2:
                    namespace = '_'.join(parts[:-1])
                    table_name = parts[-1]
                    original_tables.append(f"{namespace}.{table_name}")
    
    # Remove duplicates
    return list(set(original_tables))


def get_available_tables(settings: Optional[Settings] = None) -> dict[str, list[str]]:
    """Get list of available tables in each namespace.
    
    Args:
        settings: Application settings.
        
    Returns:
        Dictionary mapping namespace to list of table names.
    """
    settings = settings or Settings()
    catalog = get_catalog(settings)
    
    tables_by_namespace = {}
    
    # Namespaces we care about
    namespaces = ["silver", "silver_s5000f", "gold"]
    
    for namespace in namespaces:
        try:
            tables = catalog.list_tables(namespace)
            # Convert Identifier objects to strings
            tables_by_namespace[namespace] = [f"{namespace}.{t[1]}" for t in tables]
        except Exception:
            tables_by_namespace[namespace] = []
    
    return tables_by_namespace


def example_queries() -> dict[str, str]:
    """Return example SQL queries for common analytical patterns.
    
    Returns:
        Dictionary mapping query name to SQL string.
    """
    return {
        "asset_availability": """
            SELECT 
                a.id as asset_id,
                a.name as asset_name,
                a.asset_type,
                a.status as asset_status,
                ga.availability_percentage,
                ga.downtime_hours,
                ga.last_maintenance_date
            FROM silver.asset a
            LEFT JOIN gold.asset_availability ga ON a.id = ga.asset_id
            WHERE ga.date = '2024-01-01'
            ORDER BY ga.availability_percentage DESC
        """,
        
        "maintenance_history": """
            SELECT 
                a.id as asset_id,
                a.name as asset_name,
                wo.id as work_order_id,
                wo.description,
                wo.status as wo_status,
                wo.created_at as wo_created,
                wo.completed_at as wo_completed,
                COUNT(ma.id) as action_count
            FROM silver.asset a
            JOIN silver.work_order wo ON a.id = wo.asset_id
            LEFT JOIN silver.maintenance_action ma ON wo.id = ma.work_order_id
            WHERE wo.status = 'COMPLETED'
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            ORDER BY wo.completed_at DESC
        """,
        
        "s5000f_compliance": """
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
            FROM silver_s5000f.product_instance pi
            JOIN silver_s5000f.maintenance_task mt ON pi.product_instance_id = mt.product_instance_id
            LEFT JOIN silver_s5000f.maintenance_event me ON mt.maintenance_task_id = me.maintenance_task_id
            ORDER BY mt.planned_start_date
        """,
        
        "cross_layer_comparison": """
            SELECT 
                'Application' as source_layer,
                a.id as entity_id,
                a.name as entity_name,
                a.status,
                a.updated_at
            FROM silver.asset a
            UNION ALL
            SELECT 
                'S5000F' as source_layer,
                pi.product_instance_id as entity_id,
                pi.serial_number as entity_name,
                pi.status,
                pi.last_updated
            FROM silver_s5000f.product_instance pi
            ORDER BY source_layer, entity_id
        """
    }