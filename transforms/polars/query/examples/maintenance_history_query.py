"""Maintenance History Query Example.

Demonstrates querying maintenance history data across multiple tables.
This example shows how to:
1. Query maintenance history from Gold tables
2. Join Asset, WorkOrder, and MaintenanceAction tables
3. Analyze maintenance patterns and trends
4. Calculate maintenance metrics

Usage:
    python -m transforms.polars.query.examples.maintenance_history_query
"""

from __future__ import annotations

import logging

from transforms.polars.query.iceberg_query import query_iceberg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_maintenance_history_queries() -> None:
    """Run example queries for maintenance history analysis."""
    
    logger.info("=== Maintenance History Query Examples ===")
    
    # Example 1: Query gold.maintenance_history table
    logger.info("\n1. Querying gold.maintenance_history table:")
    try:
        df = query_iceberg(
            table_name="gold.maintenance_history",
            date_filter="2024-01-01",
            limit=10
        )
        logger.info(f"Retrieved {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Columns: {df.columns}")
            logger.info(f"First row:\n{df.head(1)}")
    except Exception as e:
        logger.warning(f"Could not query gold.maintenance_history: {e}")
    
    # Example 2: Detailed maintenance history with joins
    logger.info("\n2. Detailed maintenance history with asset and work order data:")
    sql_query = """
        SELECT 
            a.id as asset_id,
            a.name as asset_name,
            a.asset_type,
            wo.id as work_order_id,
            wo.description as work_description,
            wo.status as work_order_status,
            wo.priority,
            wo.created_at as wo_created,
            wo.completed_at as wo_completed,
            COUNT(ma.id) as maintenance_action_count,
            SUM(ma.duration_minutes) as total_duration_minutes
        FROM silver.asset a
        JOIN silver.work_order wo ON a.id = wo.asset_id
        LEFT JOIN silver.maintenance_action ma ON wo.id = ma.work_order_id
        WHERE wo.status = 'COMPLETED'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ORDER BY wo.completed_at DESC
        LIMIT 10
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Detailed maintenance history returned {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Results:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute detailed history query: {e}")
    
    # Example 3: Maintenance frequency by asset type
    logger.info("\n3. Maintenance frequency analysis by asset type:")
    sql_query = """
        SELECT 
            a.asset_type,
            COUNT(DISTINCT wo.id) as total_work_orders,
            COUNT(DISTINCT ma.id) as total_maintenance_actions,
            AVG(ma.duration_minutes) as avg_action_duration,
            SUM(ma.duration_minutes) as total_duration_minutes,
            MIN(wo.created_at) as first_maintenance_date,
            MAX(wo.completed_at) as last_maintenance_date
        FROM silver.asset a
        JOIN silver.work_order wo ON a.id = wo.asset_id
        LEFT JOIN silver.maintenance_action ma ON wo.id = ma.work_order_id
        WHERE wo.status = 'COMPLETED'
        GROUP BY a.asset_type
        ORDER BY total_work_orders DESC
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Maintenance frequency by asset type:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute frequency analysis query: {e}")
    
    # Example 4: Open vs completed maintenance by asset
    logger.info("\n4. Maintenance status summary by asset:")
    sql_query = """
        SELECT 
            a.id as asset_id,
            a.name as asset_name,
            a.asset_type,
            COUNT(DISTINCT CASE WHEN wo.status = 'OPEN' THEN wo.id END) as open_work_orders,
            COUNT(DISTINCT CASE WHEN wo.status = 'IN_PROGRESS' THEN wo.id END) as in_progress_work_orders,
            COUNT(DISTINCT CASE WHEN wo.status = 'COMPLETED' THEN wo.id END) as completed_work_orders,
            COUNT(DISTINCT ma.id) as total_maintenance_actions,
            SUM(CASE WHEN wo.status = 'COMPLETED' THEN ma.duration_minutes ELSE 0 END) as completed_duration_minutes
        FROM silver.asset a
        LEFT JOIN silver.work_order wo ON a.id = wo.asset_id
        LEFT JOIN silver.maintenance_action ma ON wo.id = ma.work_order_id
        GROUP BY a.id, a.name, a.asset_type
        HAVING COUNT(DISTINCT wo.id) > 0
        ORDER BY open_work_orders DESC, completed_work_orders DESC
        LIMIT 10
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Maintenance status by asset:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute status summary query: {e}")
    
    # Example 5: Maintenance backlog analysis
    logger.info("\n5. Maintenance backlog analysis:")
    sql_query = """
        SELECT 
            a.asset_type,
            COUNT(DISTINCT CASE WHEN wo.status IN ('OPEN', 'IN_PROGRESS') THEN wo.id END) as backlog_count,
            AVG(DATEDIFF('day', wo.created_at, CURRENT_DATE)) as avg_backlog_age_days,
            MAX(DATEDIFF('day', wo.created_at, CURRENT_DATE)) as max_backlog_age_days,
            SUM(CASE WHEN wo.priority = 'HIGH' THEN 1 ELSE 0 END) as high_priority_backlog,
            SUM(CASE WHEN wo.priority = 'MEDIUM' THEN 1 ELSE 0 END) as medium_priority_backlog,
            SUM(CASE WHEN wo.priority = 'LOW' THEN 1 ELSE 0 END) as low_priority_backlog
        FROM silver.asset a
        JOIN silver.work_order wo ON a.id = wo.asset_id
        WHERE wo.status IN ('OPEN', 'IN_PROGRESS')
        GROUP BY a.asset_type
        ORDER BY backlog_count DESC
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Maintenance backlog analysis:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute backlog analysis query: {e}")
    
    logger.info("\n=== Maintenance History Examples Complete ===")


if __name__ == "__main__":
    run_maintenance_history_queries()