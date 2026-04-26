"""S5000F Compliance Query Example.

Demonstrates querying S5000F-aligned data structures for compliance analysis.
This example shows how to:
1. Query Silver-S5000F tables (ProductInstance, MaintenanceTask, etc.)
2. Analyze S5000F compliance and data quality
3. Compare S5000F concepts with application entities
4. Track maintenance lifecycle in S5000F format

Usage:
    python -m transforms.polars.query.examples.s5000f_compliance_query
"""

from __future__ import annotations

import logging

from transforms.polars.query.iceberg_query import query_iceberg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_s5000f_compliance_queries() -> None:
    """Run example queries for S5000F compliance analysis."""
    
    logger.info("=== S5000F Compliance Query Examples ===")
    
    # Example 1: Query S5000F ProductInstance table
    logger.info("\n1. Querying silver_s5000f.product_instance table:")
    try:
        df = query_iceberg(
            table_name="silver_s5000f.product_instance",
            date_filter="2024-01-01",
            limit=10
        )
        logger.info(f"Retrieved {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Columns: {df.columns}")
            logger.info(f"First row:\n{df.head(1)}")
    except Exception as e:
        logger.warning(f"Could not query silver_s5000f.product_instance: {e}")
    
    # Example 2: S5000F maintenance lifecycle view
    logger.info("\n2. S5000F maintenance lifecycle view:")
    sql_query = """
        SELECT 
            pi.product_instance_id,
            pi.serial_number,
            pi.manufacturer,
            pi.model,
            pi.status as product_status,
            mt.maintenance_task_id,
            mt.task_description,
            mt.task_type,
            mt.planned_start_date,
            mt.planned_completion_date,
            mt.priority,
            me.maintenance_event_id,
            me.actual_start_date,
            me.actual_completion_date,
            me.execution_status,
            mts.step_id,
            mts.step_description,
            mts.estimated_duration_minutes,
            mts.actual_duration_minutes
        FROM silver_s5000f.product_instance pi
        LEFT JOIN silver_s5000f.maintenance_task mt ON pi.product_instance_id = mt.product_instance_id
        LEFT JOIN silver_s5000f.maintenance_event me ON mt.maintenance_task_id = me.maintenance_task_id
        LEFT JOIN silver_s5000f.maintenance_task_step mts ON me.maintenance_event_id = mts.maintenance_event_id
        ORDER BY pi.product_instance_id, mt.planned_start_date, me.actual_start_date, mts.step_sequence
        LIMIT 15
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"S5000F lifecycle view returned {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Results (first 5 rows):\n{df.head(5)}")
    except Exception as e:
        logger.warning(f"Could not execute S5000F lifecycle query: {e}")
    
    # Example 3: S5000F compliance metrics
    logger.info("\n3. S5000F compliance and data quality metrics:")
    sql_query = """
        SELECT 
            'ProductInstance' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT product_instance_id) as unique_ids,
            SUM(CASE WHEN serial_number IS NULL THEN 1 ELSE 0 END) as missing_serial_numbers,
            SUM(CASE WHEN manufacturer IS NULL THEN 1 ELSE 0 END) as missing_manufacturers,
            SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) as missing_status
        FROM silver_s5000f.product_instance
        
        UNION ALL
        
        SELECT 
            'MaintenanceTask' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT maintenance_task_id) as unique_ids,
            SUM(CASE WHEN task_description IS NULL THEN 1 ELSE 0 END) as missing_descriptions,
            SUM(CASE WHEN planned_start_date IS NULL THEN 1 ELSE 0 END) as missing_planned_start,
            SUM(CASE WHEN planned_completion_date IS NULL THEN 1 ELSE 0 END) as missing_planned_completion
        FROM silver_s5000f.maintenance_task
        
        UNION ALL
        
        SELECT 
            'MaintenanceEvent' as table_name,
            COUNT(*) as total_records,
            COUNT(DISTINCT maintenance_event_id) as unique_ids,
            SUM(CASE WHEN actual_start_date IS NULL THEN 1 ELSE 0 END) as missing_actual_start,
            SUM(CASE WHEN actual_completion_date IS NULL THEN 1 ELSE 0 END) as missing_actual_completion,
            SUM(CASE WHEN execution_status IS NULL THEN 1 ELSE 0 END) as missing_status
        FROM silver_s5000f.maintenance_event
        
        ORDER BY table_name
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"S5000F data quality metrics:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute data quality query: {e}")
    
    # Example 4: Maintenance task execution analysis
    logger.info("\n4. Maintenance task execution analysis:")
    sql_query = """
        SELECT 
            mt.task_type,
            COUNT(*) as total_tasks,
            COUNT(DISTINCT me.maintenance_event_id) as executed_tasks,
            COUNT(DISTINCT CASE WHEN me.execution_status = 'COMPLETED' THEN me.maintenance_event_id END) as completed_tasks,
            COUNT(DISTINCT CASE WHEN me.execution_status = 'IN_PROGRESS' THEN me.maintenance_event_id END) as in_progress_tasks,
            COUNT(DISTINCT CASE WHEN me.execution_status = 'PLANNED' THEN me.maintenance_event_id END) as planned_tasks,
            AVG(DATEDIFF('day', mt.planned_start_date, me.actual_start_date)) as avg_start_delay_days,
            AVG(DATEDIFF('day', mt.planned_completion_date, me.actual_completion_date)) as avg_completion_delay_days,
            AVG(mts.actual_duration_minutes) as avg_step_duration_minutes
        FROM silver_s5000f.maintenance_task mt
        LEFT JOIN silver_s5000f.maintenance_event me ON mt.maintenance_task_id = me.maintenance_task_id
        LEFT JOIN silver_s5000f.maintenance_task_step mts ON me.maintenance_event_id = mts.maintenance_event_id
        GROUP BY mt.task_type
        ORDER BY total_tasks DESC
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Maintenance task execution analysis:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute task execution analysis query: {e}")
    
    # Example 5: S5000F vs Application entity mapping verification
    logger.info("\n5. S5000F to Application entity mapping verification:")
    sql_query = """
        SELECT 
            'Asset → ProductInstance' as mapping,
            COUNT(DISTINCT a.id) as source_assets,
            COUNT(DISTINCT pi.product_instance_id) as mapped_product_instances,
            COUNT(DISTINCT CASE WHEN pi.product_instance_id IS NOT NULL THEN a.id END) as successfully_mapped,
            ROUND(
                COUNT(DISTINCT CASE WHEN pi.product_instance_id IS NOT NULL THEN a.id END) * 100.0 / 
                NULLIF(COUNT(DISTINCT a.id), 0), 
                2
            ) as mapping_percentage
        FROM silver.asset a
        LEFT JOIN silver_s5000f.product_instance pi ON a.id = pi.source_id AND pi.source_system = 'simulated-eam'
        
        UNION ALL
        
        SELECT 
            'WorkOrder → MaintenanceTask' as mapping,
            COUNT(DISTINCT wo.id) as source_work_orders,
            COUNT(DISTINCT mt.maintenance_task_id) as mapped_maintenance_tasks,
            COUNT(DISTINCT CASE WHEN mt.maintenance_task_id IS NOT NULL THEN wo.id END) as successfully_mapped,
            ROUND(
                COUNT(DISTINCT CASE WHEN mt.maintenance_task_id IS NOT NULL THEN wo.id END) * 100.0 / 
                NULLIF(COUNT(DISTINCT wo.id), 0), 
                2
            ) as mapping_percentage
        FROM silver.work_order wo
        LEFT JOIN silver_s5000f.maintenance_task mt ON wo.id = mt.source_id AND mt.source_system = 'simulated-eam'
        
        ORDER BY mapping
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"S5000F mapping verification:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute mapping verification query: {e}")
    
    logger.info("\n=== S5000F Compliance Examples Complete ===")


if __name__ == "__main__":
    run_s5000f_compliance_queries()