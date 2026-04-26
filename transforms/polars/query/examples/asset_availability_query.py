"""Asset Availability Query Example.

Demonstrates querying asset availability data using the unified Iceberg query function.
This example shows how to:
1. Query a single Gold table
2. Join Silver and Gold tables
3. Apply date filtering
4. Use Polars SQL for analytical queries

Usage:
    python -m transforms.polars.query.examples.asset_availability_query
"""

from __future__ import annotations

import logging

from transforms.polars.query.iceberg_query import query_iceberg

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_asset_availability_queries() -> None:
    """Run example queries for asset availability analysis."""
    
    logger.info("=== Asset Availability Query Examples ===")
    
    # Example 1: Simple query of gold.asset_availability table
    logger.info("\n1. Querying gold.asset_availability table:")
    try:
        df = query_iceberg(
            table_name="gold.asset_availability",
            date_filter="2024-01-01",
            limit=10
        )
        logger.info(f"Retrieved {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Columns: {df.columns}")
            logger.info(f"First row:\n{df.head(1)}")
    except Exception as e:
        logger.warning(f"Could not query gold.asset_availability: {e}")
    
    # Example 2: Simple join between Silver asset and Gold availability
    logger.info("\n2. Simple join between Silver asset and Gold availability:")
    sql_query = """
        SELECT 
            a.id as asset_id,
            a.name as asset_name,
            a.asset_type,
            a.status as asset_status,
            ga.availability_pct,
            ga.is_available,
            ga.computed_at
        FROM silver_asset a
        LEFT JOIN gold_asset_availability ga ON a.id = ga.asset_id
        LIMIT 5
    """
    
    try:
        df = query_iceberg(sql=sql_query)
        logger.info(f"Join query returned {len(df)} rows")
        if len(df) > 0:
            logger.info(f"Results:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute join query: {e}")
    
    # Example 3: Asset summary by type
    logger.info("\n3. Asset summary by type:")
    sql_query = """
        SELECT 
            a.asset_type,
            COUNT(*) as total_assets,
            AVG(CASE WHEN ga.is_available THEN 100.0 ELSE 0.0 END) as avg_availability_pct,
            SUM(CASE WHEN ga.is_available THEN 1 ELSE 0 END) as available_count,
            SUM(CASE WHEN NOT ga.is_available THEN 1 ELSE 0 END) as unavailable_count
        FROM silver_asset a
        LEFT JOIN gold_asset_availability ga ON a.id = ga.asset_id
        GROUP BY a.asset_type
        ORDER BY total_assets DESC
    """
    
    try:
        df = query_iceberg(sql=sql_query)
        logger.info(f"Summary by asset type:\n{df}")
    except Exception as e:
        logger.warning(f"Could not execute summary query: {e}")
    
    # Example 4: Assets needing attention
    logger.info("\n4. Assets needing attention (not available):")
    sql_query = """
        SELECT 
            a.id as asset_id,
            a.name as asset_name,
            a.asset_type,
            a.location,
            a.status,
            ga.availability_pct,
            ga.is_available,
            CASE 
                WHEN ga.is_available THEN 'HEALTHY'
                ELSE 'NEEDS_ATTENTION'
            END as health_status
        FROM silver_asset a
        LEFT JOIN gold_asset_availability ga ON a.id = ga.asset_id
        WHERE NOT ga.is_available OR ga.is_available IS NULL
        ORDER BY a.asset_type, a.name
        LIMIT 10
    """
    
    try:
        df = query_iceberg(sql=sql_query)
        if len(df) > 0:
            logger.info(f"Assets needing attention:\n{df}")
        else:
            logger.info("All assets are available")
    except Exception as e:
        logger.warning(f"Could not execute attention query: {e}")
    
    logger.info("\n=== Asset Availability Examples Complete ===")


if __name__ == "__main__":
    run_asset_availability_queries()