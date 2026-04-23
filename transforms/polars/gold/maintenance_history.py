"""Maintenance History — Gold analytics metric.

Provides a per-asset view of maintenance events from the S5000F layer,
showing how many maintenance events each asset has undergone.

Output columns:
    product_instance_id, asset_source_id, event_count,
    first_maintenance, last_maintenance

Usage:
    python -m transforms.polars.gold.maintenance_history
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


def compute_maintenance_history() -> int:
    """Compute maintenance history per asset from S5000F MaintenanceEvent data.

    Returns:
        Number of records written to the Gold table.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "gold")

    try:
        event_table = catalog.load_table("silver_s5000f.maintenance_event")
        events = read_iceberg_table(event_table).collect()
    except Exception:
        logger.warning("Silver-S5000F maintenance_event table not found")
        return 0

    if events.is_empty():
        return 0

    computed_at = datetime.now(timezone.utc).isoformat()
    result = (
        events
        .group_by("product_instance_id")
        .agg(
            pl.col("source_id").first().alias("asset_source_id"),
            pl.col("maintenance_event_id").count().alias("event_count"),
            pl.col("actual_start").min().alias("first_maintenance"),
            pl.col("actual_start").max().alias("last_maintenance"),
            pl.col("action_count").sum().alias("total_actions"),
        )
        .with_columns(pl.lit(computed_at).alias("computed_at"))
        .sort("product_instance_id")
    )

    try:
        gold_table = catalog.load_table("gold.maintenance_history")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        schema = Schema(
            NestedField(1, "product_instance_id", StringType()),
            NestedField(2, "asset_source_id", StringType()),
            NestedField(3, "event_count", LongType()),
            NestedField(4, "first_maintenance", StringType()),
            NestedField(5, "last_maintenance", StringType()),
            NestedField(6, "total_actions", LongType()),
            NestedField(7, "computed_at", StringType()),
        )
        gold_table = catalog.create_table("gold.maintenance_history", schema=schema)

    write_iceberg_overwrite(gold_table, result)
    logger.info("Gold maintenance_history: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = compute_maintenance_history()
    logger.info("Done. %d records.", count)
