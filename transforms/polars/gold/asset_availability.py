"""Asset Availability — Gold analytics metric.

Calculates the operational availability of each asset based on its
current status in the Silver layer. In a production system this would
use time-series snapshots; for this PoC we derive a point-in-time view.

Output columns:
    asset_id, name, asset_type, location, status,
    is_available (bool), availability_pct (simplified)

Usage:
    python -m transforms.polars.gold.asset_availability
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import polars as pl

from config.settings import Settings
from transforms.polars.app.iceberg_io import (
    ensure_namespace,
    get_catalog,
    read_iceberg_table,
    write_iceberg_overwrite,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def compute_asset_availability() -> int:
    """Compute asset availability metrics from Silver asset data.

    Returns:
        Number of records written to the Gold table.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "gold")

    try:
        silver_table = catalog.load_table("silver.asset")
        assets = read_iceberg_table(silver_table).collect()
    except Exception:
        logger.warning("Silver asset table not found")
        return 0

    if assets.is_empty():
        return 0

    # Point-in-time availability: operational = available
    computed_at = datetime.now(timezone.utc).isoformat()
    result = assets.select(
        pl.col("id").alias("asset_id"),
        pl.col("name"),
        pl.col("asset_type"),
        pl.col("location"),
        pl.col("status"),
        (pl.col("status") == "operational").alias("is_available"),
    ).with_columns(
        # Simplified availability: 100% if operational, 0% otherwise
        pl.when(pl.col("is_available"))
        .then(pl.lit(100.0))
        .otherwise(pl.lit(0.0))
        .alias("availability_pct"),
        pl.lit(computed_at).alias("computed_at"),
    )

    try:
        gold_table = catalog.load_table("gold.asset_availability")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import BooleanType, DoubleType, NestedField, StringType

        schema = Schema(
            NestedField(1, "asset_id", StringType()),
            NestedField(2, "name", StringType()),
            NestedField(3, "asset_type", StringType()),
            NestedField(4, "location", StringType()),
            NestedField(5, "status", StringType()),
            NestedField(6, "is_available", BooleanType()),
            NestedField(7, "availability_pct", DoubleType()),
            NestedField(8, "computed_at", StringType()),
        )
        gold_table = catalog.create_table("gold.asset_availability", schema=schema)

    write_iceberg_overwrite(gold_table, result)
    logger.info("Gold asset_availability: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = compute_asset_availability()
    logger.info("Done. %d records.", count)
