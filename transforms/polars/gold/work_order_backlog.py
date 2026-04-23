"""Work Order Backlog — Gold analytics metric.

Summarises open (non-completed, non-cancelled) work orders by priority
and asset, providing a view of the current maintenance backlog.

Output columns:
    priority, asset_id, open_count, oldest_planned_start

Usage:
    python -m transforms.polars.gold.work_order_backlog
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


def compute_work_order_backlog() -> int:
    """Compute work order backlog metrics.

    Returns:
        Number of records written to the Gold table.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "gold")

    # Use Silver work_request to get priority, join with work_order
    try:
        wo_table = catalog.load_table("silver.work_order")
        wo_df = read_iceberg_table(wo_table).collect()
    except Exception:
        logger.warning("Silver work_order table not found")
        return 0

    try:
        wr_table = catalog.load_table("silver.work_request")
        wr_df = read_iceberg_table(wr_table).collect()
    except Exception:
        wr_df = pl.DataFrame()

    if wo_df.is_empty():
        return 0

    # Filter to open work orders (planned or in_progress)
    open_wo = wo_df.filter(pl.col("status").is_in(["planned", "in_progress"]))

    if open_wo.is_empty():
        logger.info("No open work orders — backlog is empty")
        return 0

    # Join with work requests to get priority
    if not wr_df.is_empty():
        open_wo = open_wo.join(
            wr_df.select(pl.col("id").alias("wr_id"), "priority"),
            left_on="work_request_id",
            right_on="wr_id",
            how="left",
        )
    else:
        open_wo = open_wo.with_columns(pl.lit("unknown").alias("priority"))

    computed_at = datetime.now(timezone.utc).isoformat()
    result = (
        open_wo
        .group_by("priority", "asset_id")
        .agg(
            pl.col("id").count().alias("open_count"),
            pl.col("planned_start").min().alias("oldest_planned_start"),
        )
        .with_columns(pl.lit(computed_at).alias("computed_at"))
        .sort("priority", "asset_id")
    )

    try:
        gold_table = catalog.load_table("gold.work_order_backlog")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        schema = Schema(
            NestedField(1, "priority", StringType()),
            NestedField(2, "asset_id", StringType()),
            NestedField(3, "open_count", LongType()),
            NestedField(4, "oldest_planned_start", StringType()),
            NestedField(5, "computed_at", StringType()),
        )
        gold_table = catalog.create_table("gold.work_order_backlog", schema=schema)

    write_iceberg_overwrite(gold_table, result)
    logger.info("Gold work_order_backlog: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = compute_work_order_backlog()
    logger.info("Done. %d records.", count)
