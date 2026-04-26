"""Iceberg Query Module — Unified query interface for analytical exploration.

Provides reusable Polars SQL query functions for querying Silver, Silver-S5000F,
and Gold Iceberg tables with automatic partition filtering and cross-layer joins.

Main Functions:
    query_iceberg: Unified query function for Iceberg tables
    get_available_tables: List available tables in each namespace
    example_queries: Get example SQL queries for common patterns

Example Usage:
    from transforms.polars.query import query_iceberg
    
    # Query a single table
    df = query_iceberg("silver.asset", date_filter="2024-01-01")
    
    # Execute custom SQL
    df = query_iceberg(
        sql=\"\"\"
        SELECT a.id, a.name, wo.status
        FROM silver.asset a
        JOIN silver.work_order wo ON a.id = wo.asset_id
        \"\"\",
        date_filter="2024-01-01"
    )
"""

from transforms.polars.query.iceberg_query import (
    query_iceberg,
    get_available_tables,
    example_queries,
)

__all__ = [
    "query_iceberg",
    "get_available_tables", 
    "example_queries",
]