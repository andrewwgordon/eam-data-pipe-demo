"""MaintenanceTaskStep transform — Silver MaintenanceAction → Silver-S5000F.

Maps application MaintenanceAction entities to the S5000F MaintenanceTaskStep
concept, representing an atomic work activity within a maintenance task.

Mapping:
    MaintenanceAction.id             → task_step_id (deterministic hash)
    MaintenanceAction.work_order_id  → maintenance_task_id (deterministic hash of WO)
    MaintenanceAction.step_number    → step_number
    MaintenanceAction.description    → description
    MaintenanceAction.status         → status
    MaintenanceAction.started_at     → started_at
    MaintenanceAction.completed_at   → completed_at

Usage:
    python -m transforms.polars.s5000f.maintenance_task_step
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


def transform_maintenance_task_step() -> int:
    """Transform Silver maintenance actions into S5000F MaintenanceTaskStep records.

    Returns:
        Number of MaintenanceTaskStep records written.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver_s5000f")

    try:
        silver_table = catalog.load_table("silver.maintenance_action")
        silver_df = read_iceberg_table(silver_table).collect()
    except Exception:
        logger.warning("Silver maintenance_action table not found — nothing to transform")
        return 0

    if silver_df.is_empty():
        logger.info("Silver maintenance_action table is empty")
        return 0

    mapped_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for row in silver_df.iter_rows(named=True):
        rows.append({
            "task_step_id": s5000f_id("MaintenanceTaskStep", row["id"]),
            "maintenance_task_id": s5000f_id("MaintenanceTask", row["work_order_id"]),
            "step_number": row["step_number"],
            "description": row.get("description", ""),
            "status": row["status"],
            "started_at": row.get("started_at", ""),
            "completed_at": row.get("completed_at", ""),
            "source_system": row["source_system"],
            "source_id": row["source_id"],
            "mapped_at": mapped_at,
        })

    result = pl.DataFrame(rows)

    try:
        s5000f_table = catalog.load_table("silver_s5000f.maintenance_task_step")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        schema = Schema(
            NestedField(1, "task_step_id", StringType()),
            NestedField(2, "maintenance_task_id", StringType()),
            NestedField(3, "step_number", LongType()),
            NestedField(4, "description", StringType()),
            NestedField(5, "status", StringType()),
            NestedField(6, "started_at", StringType()),
            NestedField(7, "completed_at", StringType()),
            NestedField(8, "source_system", StringType()),
            NestedField(9, "source_id", StringType()),
            NestedField(10, "mapped_at", StringType()),
        )
        s5000f_table = catalog.create_table(
            "silver_s5000f.maintenance_task_step", schema=schema
        )

    write_iceberg_overwrite(s5000f_table, result)
    logger.info("Silver-S5000F maintenance_task_step: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = transform_maintenance_task_step()
    logger.info("Done. %d MaintenanceTaskStep records.", count)
