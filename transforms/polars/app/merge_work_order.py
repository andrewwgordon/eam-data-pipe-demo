"""Merge WorkOrder — Bronze CDC → Silver current-state table.

Resolves WorkOrder CDC events into a single-row-per-order Silver table.

Usage:
    python -m transforms.polars.app.merge_work_order
"""

from __future__ import annotations

import logging

from config.settings import Settings
from transforms.polars.app.cdc_merge import resolve_cdc
from transforms.polars.app.iceberg_io import (
    SILVER_WORK_ORDER_SCHEMA,
    ensure_namespace,
    get_catalog,
    read_iceberg_table,
    write_iceberg_overwrite,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WORK_ORDER_COLUMNS = [
    "id", "work_request_id", "asset_id", "status",
    "planned_start", "planned_end", "actual_start", "actual_end", "updated_at",
]


def merge_work_order() -> int:
    """Resolve WorkOrder CDC events from Bronze into Silver.

    Returns:
        Number of records in the resulting Silver table.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver")

    try:
        bronze_table = catalog.load_table("bronze.work_order")
        bronze_df = read_iceberg_table(bronze_table)
    except Exception:
        logger.warning("Bronze work_order table not found — nothing to merge")
        return 0

    try:
        silver_table = catalog.load_table("silver.work_order")
        existing_silver = read_iceberg_table(silver_table)
    except Exception:
        silver_table = catalog.create_table(
            "silver.work_order", schema=SILVER_WORK_ORDER_SCHEMA
        )
        existing_silver = None

    result = resolve_cdc(bronze_df, existing_silver, WORK_ORDER_COLUMNS)

    if result.is_empty():
        logger.info("No work order records to write")
        return 0

    write_iceberg_overwrite(silver_table, result)
    logger.info("Silver work_order table updated: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = merge_work_order()
    logger.info("Done. %d work order records in Silver.", count)
