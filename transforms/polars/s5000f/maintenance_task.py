"""MaintenanceTask transform — Silver WorkOrder → Silver-S5000F MaintenanceTask.

Maps application WorkOrder entities to the S5000F MaintenanceTask concept,
representing planned maintenance activity.

Mapping:
    WorkOrder.id               → maintenance_task_id (deterministic hash)
    WorkOrder.asset_id         → product_instance_id (deterministic hash of asset)
    WorkOrder.work_request_id  → functional_failure_id (deterministic hash of WR)
    WorkOrder.status           → status
    WorkOrder.planned_start    → planned_start
    WorkOrder.planned_end      → planned_end

Usage:
    python -m transforms.polars.s5000f.maintenance_task
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


def transform_maintenance_task() -> int:
    """Transform Silver work orders into S5000F MaintenanceTask records.

    Returns:
        Number of MaintenanceTask records written.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver_s5000f")

    try:
        silver_table = catalog.load_table("silver.work_order")
        silver_df = read_iceberg_table(silver_table).collect()
    except Exception:
        logger.warning("Silver work_order table not found — nothing to transform")
        return 0

    if silver_df.is_empty():
        logger.info("Silver work_order table is empty")
        return 0

    mapped_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for row in silver_df.iter_rows(named=True):
        rows.append({
            "maintenance_task_id": s5000f_id("MaintenanceTask", row["id"]),
            "product_instance_id": s5000f_id("ProductInstance", row["asset_id"]),
            "functional_failure_id": s5000f_id("FunctionalFailure", row["work_request_id"]),
            "status": row["status"],
            "planned_start": row.get("planned_start", ""),
            "planned_end": row.get("planned_end", ""),
            "source_system": row["source_system"],
            "source_id": row["source_id"],
            "mapped_at": mapped_at,
        })

    result = pl.DataFrame(rows)

    try:
        s5000f_table = catalog.load_table("silver_s5000f.maintenance_task")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType

        schema = Schema(
            NestedField(1, "maintenance_task_id", StringType()),
            NestedField(2, "product_instance_id", StringType()),
            NestedField(3, "functional_failure_id", StringType()),
            NestedField(4, "status", StringType()),
            NestedField(5, "planned_start", StringType()),
            NestedField(6, "planned_end", StringType()),
            NestedField(7, "source_system", StringType()),
            NestedField(8, "source_id", StringType()),
            NestedField(9, "mapped_at", StringType()),
        )
        s5000f_table = catalog.create_table("silver_s5000f.maintenance_task", schema=schema)

    write_iceberg_overwrite(s5000f_table, result)
    logger.info("Silver-S5000F maintenance_task: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = transform_maintenance_task()
    logger.info("Done. %d MaintenanceTask records.", count)
