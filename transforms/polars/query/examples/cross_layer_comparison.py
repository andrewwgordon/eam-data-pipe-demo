"""Cross-Layer Comparison Query Example.

Demonstrates querying and comparing data across different data layers:
- Silver (Application schema)
- Silver-S5000F (Standardised schema) 
- Gold (Analytics)

This example shows how to:
1. Query multiple data layers in a single query
2. Compare application vs S5000F representations
3. Analyze data consistency across layers
4. Generate unified reports from multiple sources

Usage:
    python -m transforms.polars.query.examples.cross_layer_comparison
"""

from __future__ import annotations

import logging

from transforms.polars.query.iceberg_query import query_iceberg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_cross_layer_comparison_queries() -> None:
    """Run example queries for cross-layer comparison analysis."""
    
    logger.info("=== Cross-Layer Comparison Query Examples ===")
    
    # Example 1: Basic cross-layer entity comparison
    logger.info("\n1. Basic entity comparison across layers:")
    sql_query = """
        SELECT 
            'Silver (Application)' as data_layer,
            a.id as entity_id,
            a.name as entity_name,
            a.asset_type,
            a.status,
            a.location,
            a.install_date,
            NULL as serial_number,
            NULL as manufacturer,
            NULL as model
        FROM silver.asset a
        
        UNION ALL
        
        SELECT 
            'Silver-S5000F (Standardised)' as data_layer,
            pi.product_instance_id as entity_id,
            pi.serial_number as entity_name,
            NULL as asset_type,
            pi.status,
            NULL as location,
            NULL as install_date,
            pi.serial_number,
            pi.manufacturer,
            pi.model
        FROM silver_s5000f.product_instance pi
        
        ORDER BY data_layer, entity_id
        LIMIT 20
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Cross-layer entity comparison returned {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Results (first 10 rows):\n{df.head(10)}")
    except Exception as e:
        logger.warning(f"Could not execute cross-layer comparison query: {e}")
    
    # Example 2: Maintenance data comparison
    logger.info("\n2. Maintenance data comparison across layers:")
    sql_query = """
        SELECT 
            'Application Maintenance' as source,
            wo.id as maintenance_id,
            wo.description,
            wo.status,
            wo.priority,
            wo.created_at as planned_date,
            wo.completed_at as completed_date,
            a.name as asset_name,
            COUNT(ma.id) as action_count,
            SUM(ma.duration_minutes) as total_duration
        FROM silver.work_order wo
        JOIN silver.asset a ON wo.asset_id = a.id
        LEFT JOIN silver.maintenance_action ma ON wo.id = ma.work_order_id
        WHERE wo.status = 'COMPLETED'
        GROUP BY wo.id, wo.description, wo.status, wo.priority, wo.created_at, wo.completed_at, a.name
        
        UNION ALL
        
        SELECT 
            'S5000F Maintenance' as source,
            mt.maintenance_task_id as maintenance_id,
            mt.task_description as description,
            me.execution_status as status,
            mt.priority,
            mt.planned_start_date as planned_date,
            me.actual_completion_date as completed_date,
            pi.serial_number as asset_name,
            COUNT(mts.step_id) as action_count,
            SUM(mts.actual_duration_minutes) as total_duration
        FROM silver_s5000f.maintenance_task mt
        JOIN silver_s5000f.product_instance pi ON mt.product_instance_id = pi.product_instance_id
        LEFT JOIN silver_s5000f.maintenance_event me ON mt.maintenance_task_id = me.maintenance_task_id
        LEFT JOIN silver_s5000f.maintenance_task_step mts ON me.maintenance_event_id = mts.maintenance_event_id
        WHERE me.execution_status = 'COMPLETED'
        GROUP BY mt.maintenance_task_id, mt.task_description, me.execution_status, mt.priority, 
                 mt.planned_start_date, me.actual_completion_date, pi.serial_number
        
        ORDER BY source, completed_date DESC
        LIMIT 15
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Maintenance data comparison returned {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Results:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute maintenance comparison query: {e}")
    
    # Example 3: Data completeness comparison
    logger.info("\n3. Data completeness comparison across layers:")
    sql_query = """
        SELECT 
            'Silver' as layer,
            'Asset' as entity_type,
            COUNT(*) as total_records,
            AVG(CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END) * 100 as name_completeness,
            AVG(CASE WHEN asset_type IS NOT NULL THEN 1 ELSE 0 END) * 100 as type_completeness,
            AVG(CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END) * 100 as status_completeness,
            AVG(CASE WHEN location IS NOT NULL THEN 1 ELSE 0 END) * 100 as location_completeness
        FROM silver.asset
        
        UNION ALL
        
        SELECT 
            'Silver-S5000F' as layer,
            'ProductInstance' as entity_type,
            COUNT(*) as total_records,
            AVG(CASE WHEN serial_number IS NOT NULL THEN 1 ELSE 0 END) * 100 as serial_completeness,
            AVG(CASE WHEN manufacturer IS NOT NULL THEN 1 ELSE 0 END) * 100 as manufacturer_completeness,
            AVG(CASE WHEN model IS NOT NULL THEN 1 ELSE 0 END) * 100 as model_completeness,
            AVG(CASE WHEN status IS NOT NULL THEN 1 ELSE 0 END) * 100 as status_completeness
        FROM silver_s5000f.product_instance
        
        UNION ALL
        
        SELECT 
            'Gold' as layer,
            'AssetAvailability' as entity_type,
            COUNT(*) as total_records,
            AVG(CASE WHEN asset_id IS NOT NULL THEN 1 ELSE 0 END) * 100 as asset_id_completeness,
            AVG(CASE WHEN availability_percentage IS NOT NULL THEN 1 ELSE 0 END) * 100 as availability_completeness,
            AVG(CASE WHEN downtime_hours IS NOT NULL THEN 1 ELSE 0 END) * 100 as downtime_completeness,
            AVG(CASE WHEN last_maintenance_date IS NOT NULL THEN 1 ELSE 0 END) * 100 as maintenance_date_completeness
        FROM gold.asset_availability
        
        ORDER BY layer, entity_type
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Data completeness comparison:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute data completeness query: {e}")
    
    # Example 4: Timeline comparison - application vs S5000F events
    logger.info("\n4. Timeline comparison of maintenance events:")
    sql_query = """
        SELECT 
            'Application Event' as event_type,
            ma.id as event_id,
            ma.description,
            ma.started_at as event_time,
            a.name as asset_name,
            wo.description as work_description,
            ma.duration_minutes
        FROM silver.maintenance_action ma
        JOIN silver.work_order wo ON ma.work_order_id = wo.id
        JOIN silver.asset a ON wo.asset_id = a.id
        WHERE ma.started_at IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'S5000F Event' as event_type,
            mts.step_id as event_id,
            mts.step_description as description,
            me.actual_start_date as event_time,
            pi.serial_number as asset_name,
            mt.task_description as work_description,
            mts.actual_duration_minutes as duration_minutes
        FROM silver_s5000f.maintenance_task_step mts
        JOIN silver_s5000f.maintenance_event me ON mts.maintenance_event_id = me.maintenance_event_id
        JOIN silver_s5000f.maintenance_task mt ON me.maintenance_task_id = mt.maintenance_task_id
        JOIN silver_s5000f.product_instance pi ON mt.product_instance_id = pi.product_instance_id
        WHERE me.actual_start_date IS NOT NULL
        
        ORDER BY event_time DESC
        LIMIT 20
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Timeline comparison returned {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Results:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute timeline comparison query: {e}")
    
    # Example 5: Unified analytics dashboard query
    logger.info("\n5. Unified analytics dashboard (all layers):")
    sql_query = """
        WITH asset_summary AS (
            SELECT 
                a.id as asset_id,
                a.name as asset_name,
                a.asset_type,
                a.status as asset_status,
                a.location,
                pi.serial_number,
                pi.manufacturer,
                pi.model as s5000f_model,
                ga.availability_percentage,
                ga.downtime_hours,
                ga.last_maintenance_date,
                COUNT(DISTINCT wo.id) as total_work_orders,
                COUNT(DISTINCT CASE WHEN wo.status = 'COMPLETED' THEN wo.id END) as completed_work_orders,
                COUNT(DISTINCT CASE WHEN wo.status IN ('OPEN', 'IN_PROGRESS') THEN wo.id END) as pending_work_orders
            FROM silver.asset a
            LEFT JOIN silver_s5000f.product_instance pi ON a.id = pi.source_id AND pi.source_system = 'simulated-eam'
            LEFT JOIN gold.asset_availability ga ON a.id = ga.asset_id AND ga.date = '2024-01-01'
            LEFT JOIN silver.work_order wo ON a.id = wo.asset_id
            GROUP BY a.id, a.name, a.asset_type, a.status, a.location, 
                     pi.serial_number, pi.manufacturer, pi.s5000f_model,
                     ga.availability_percentage, ga.downtime_hours, ga.last_maintenance_date
        ),
        maintenance_summary AS (
            SELECT 
                a.id as asset_id,
                COUNT(DISTINCT mt.maintenance_task_id) as s5000f_maintenance_tasks,
                COUNT(DISTINCT me.maintenance_event_id) as s5000f_maintenance_events,
                COUNT(DISTINCT mts.step_id) as s5000f_maintenance_steps,
                AVG(mts.actual_duration_minutes) as avg_s5000f_step_duration
            FROM silver.asset a
            LEFT JOIN silver_s5000f.product_instance pi ON a.id = pi.source_id AND pi.source_system = 'simulated-eam'
            LEFT JOIN silver_s5000f.maintenance_task mt ON pi.product_instance_id = mt.product_instance_id
            LEFT JOIN silver_s5000f.maintenance_event me ON mt.maintenance_task_id = me.maintenance_task_id
            LEFT JOIN silver_s5000f.maintenance_task_step mts ON me.maintenance_event_id = mts.maintenance_event_id
            GROUP BY a.id
        )
        SELECT 
            asum.asset_id,
            asum.asset_name,
            asum.asset_type,
            asum.asset_status,
            asum.location,
            asum.serial_number,
            asum.manufacturer,
            asum.availability_percentage,
            asum.downtime_hours,
            asum.total_work_orders,
            asum.completed_work_orders,
            asum.pending_work_orders,
            msum.s5000f_maintenance_tasks,
            msum.s5000f_maintenance_events,
            msum.s5000f_maintenance_steps,
            msum.avg_s5000f_step_duration,
            CASE 
                WHEN asum.availability_percentage >= 95 THEN 'EXCELLENT'
                WHEN asum.availability_percentage >= 90 THEN 'GOOD'
                WHEN asum.availability_percentage >= 80 THEN 'FAIR'
                ELSE 'POOR'
            END as health_status
        FROM asset_summary asum
        LEFT JOIN maintenance_summary msum ON asum.asset_id = msum.asset_id
        ORDER BY asum.availability_percentage DESC NULLS LAST, asum.asset_name
        LIMIT 15
    """
    
    try:
        df = query_iceberg(
            sql=sql_query,
            date_filter="2024-01-01"
        )
        logger.info(f"Unified analytics dashboard returned {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Results:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute unified analytics query: {e}")
    
    logger.info("\n=== Cross-Layer Comparison Examples Complete ===")


if __name__ == "__main__":
    run_cross_layer_comparison_queries()