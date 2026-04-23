"""MTBF Metrics — Gold analytics metric.

Simplified mean-time-between-failure (MTBF) calculation by asset type.
Uses the S5000F FunctionalFailure table to count failures per asset type,
then derives a simplified MTBF proxy.

Note: This is a PoC approximation. Real MTBF requires time-series data
with precise failure and restoration timestamps.

Output columns:
    asset_type, failure_count, asset_count, failures_per_asset, mtbf_proxy_hours

Usage:
    python -m transforms.polars.gold.mtbf_metrics
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


def compute_mtbf_metrics() -> int:
    """Compute simplified MTBF metrics by asset type.

    Returns:
        Number of records written to the Gold table.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "gold")

    # Load S5000F product instances (for asset type) and functional failures
    try:
        pi_table = catalog.load_table("silver_s5000f.product_instance")
        pi_df = read_iceberg_table(pi_table).collect()
    except Exception:
        logger.warning("Silver-S5000F product_instance table not found")
        return 0

    try:
        ff_table = catalog.load_table("silver_s5000f.functional_failure")
        ff_df = read_iceberg_table(ff_table).collect()
    except Exception:
        ff_df = pl.DataFrame()

    if pi_df.is_empty():
        return 0

    # Count assets per type
    asset_counts = (
        pi_df
        .group_by("type")
        .agg(pl.col("product_instance_id").count().alias("asset_count"))
    )

    # Count failures per asset type (join failures → product instances)
    if not ff_df.is_empty():
        failures_with_type = ff_df.join(
            pi_df.select("product_instance_id", "type"),
            on="product_instance_id",
            how="left",
        )
        failure_counts = (
            failures_with_type
            .group_by("type")
            .agg(pl.col("functional_failure_id").count().alias("failure_count"))
        )
    else:
        failure_counts = pl.DataFrame({"type": [], "failure_count": []})

    # Merge and compute MTBF proxy
    computed_at = datetime.now(timezone.utc).isoformat()
    result = (
        asset_counts
        .join(failure_counts, on="type", how="left")
        .with_columns(
            pl.col("failure_count").fill_null(0),
        )
        .with_columns(
            (pl.col("failure_count") / pl.col("asset_count")).alias("failures_per_asset"),
            # MTBF proxy: assume 8760 hours/year operating time, divide by failures
            pl.when(pl.col("failure_count") > 0)
            .then(pl.lit(8760.0) * pl.col("asset_count") / pl.col("failure_count"))
            .otherwise(pl.lit(None))
            .alias("mtbf_proxy_hours"),
        )
        .with_columns(pl.lit(computed_at).alias("computed_at"))
        .sort("type")
    )

    try:
        gold_table = catalog.load_table("gold.mtbf_metrics")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import DoubleType, LongType, NestedField, StringType

        schema = Schema(
            NestedField(1, "type", StringType()),
            NestedField(2, "asset_count", LongType()),
            NestedField(3, "failure_count", LongType()),
            NestedField(4, "failures_per_asset", DoubleType()),
            NestedField(5, "mtbf_proxy_hours", DoubleType()),
            NestedField(6, "computed_at", StringType()),
        )
        gold_table = catalog.create_table("gold.mtbf_metrics", schema=schema)

    write_iceberg_overwrite(gold_table, result)
    logger.info("Gold mtbf_metrics: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = compute_mtbf_metrics()
    logger.info("Done. %d records.", count)
