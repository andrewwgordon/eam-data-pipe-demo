"""MaintenanceEvent transform — Silver WorkOrder + Actions → Silver-S5000F.

Maps completed WorkOrders (with their MaintenanceActions) to the S5000F
MaintenanceEvent concept, representing executed maintenance.

CRITICAL RULE (AGENT.md §6.3):
    Only work orders with status='completed' AND actual_start IS NOT NULL
    produce MaintenanceEvents. Open or unexecuted work orders are excluded.

Mapping:
    WorkOrder.id         → maintenance_event_id (deterministic hash)
    WorkOrder.id         → maintenance_task_id (deterministic hash)
    WorkOrder.asset_id   → product_instance_id (deterministic hash of asset)
    WorkOrder.actual_*   → actual_start, actual_end
    COUNT(actions)       → action_count

Usage:
    python -m transforms.polars.s5000f.maintenance_event
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
from transforms.polars.s5000f.id_utils import s5000f_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def transform_maintenance_event() -> int:
    """Transform completed Silver work orders into S5000F MaintenanceEvent records.

    Only work orders with status='completed' and a non-empty actual_start
    produce events. This enforces the temporal rule from AGENT.md §6.3.

    Returns:
        Number of MaintenanceEvent records written.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver_s5000f")

    # Load work orders
    try:
        wo_table = catalog.load_table("silver.work_order")
        wo_df = read_iceberg_table(wo_table).collect()
    except Exception:
        logger.warning("Silver work_order table not found — nothing to transform")
        return 0

    # Load maintenance actions (for action count)
    try:
        action_table = catalog.load_table("silver.maintenance_action")
        action_df = read_iceberg_table(action_table).collect()
    except Exception:
        action_df = pl.DataFrame()

    if wo_df.is_empty():
        logger.info("Silver work_order table is empty")
        return 0

    # Filter: only completed work orders with actual_start
    completed = wo_df.filter(
        (pl.col("status") == "completed")
        & (pl.col("actual_start").is_not_null())
        & (pl.col("actual_start") != "")
    )

    if completed.is_empty():
        logger.info("No completed work orders with actual_start — no events to generate")
        return 0

    # Count actions per work order
    action_counts: dict[str, int] = {}
    if not action_df.is_empty():
        counts = (
            action_df
            .group_by("work_order_id")
            .agg(pl.col("id").count().alias("action_count"))
        )
        for row in counts.iter_rows(named=True):
            action_counts[row["work_order_id"]] = row["action_count"]

    mapped_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for row in completed.iter_rows(named=True):
        wo_id = row["id"]
        rows.append({
            "maintenance_event_id": s5000f_id("MaintenanceEvent", wo_id),
            "maintenance_task_id": s5000f_id("MaintenanceTask", wo_id),
            "product_instance_id": s5000f_id("ProductInstance", row["asset_id"]),
            "actual_start": row.get("actual_start", ""),
            "actual_end": row.get("actual_end", ""),
            "action_count": action_counts.get(wo_id, 0),
            "source_system": row["source_system"],
            "source_id": row["source_id"],
            "mapped_at": mapped_at,
        })

    result = pl.DataFrame(rows)

    try:
        s5000f_table = catalog.load_table("silver_s5000f.maintenance_event")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        schema = Schema(
            NestedField(1, "maintenance_event_id", StringType()),
            NestedField(2, "maintenance_task_id", StringType()),
            NestedField(3, "product_instance_id", StringType()),
            NestedField(4, "actual_start", StringType()),
            NestedField(5, "actual_end", StringType()),
            NestedField(6, "action_count", LongType()),
            NestedField(7, "source_system", StringType()),
            NestedField(8, "source_id", StringType()),
            NestedField(9, "mapped_at", StringType()),
        )
        s5000f_table = catalog.create_table("silver_s5000f.maintenance_event", schema=schema)

    write_iceberg_overwrite(s5000f_table, result)
    logger.info("Silver-S5000F maintenance_event: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = transform_maintenance_event()
    logger.info("Done. %d MaintenanceEvent records.", count)
