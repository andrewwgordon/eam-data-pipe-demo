"""Merge MaintenanceAction — Bronze CDC → Silver current-state table.

Resolves MaintenanceAction CDC events into a single-row-per-action Silver table.

Usage:
    python -m transforms.polars.app.merge_maintenance_action
"""

from __future__ import annotations

import logging

from config.settings import Settings
from transforms.polars.app.cdc_merge import resolve_cdc
from transforms.polars.app.iceberg_io import (
    SILVER_MAINTENANCE_ACTION_SCHEMA,
    ensure_namespace,
    get_catalog,
    read_iceberg_table,
    write_iceberg_overwrite,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MAINTENANCE_ACTION_COLUMNS = [
    "id", "work_order_id", "step_number", "description",
    "status", "started_at", "completed_at",
]


def merge_maintenance_action() -> int:
    """Resolve MaintenanceAction CDC events from Bronze into Silver.

    Returns:
        Number of records in the resulting Silver table.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver")

    try:
        bronze_table = catalog.load_table("bronze.maintenance_action")
        bronze_df = read_iceberg_table(bronze_table)
    except Exception:
        logger.warning("Bronze maintenance_action table not found — nothing to merge")
        return 0

    try:
        silver_table = catalog.load_table("silver.maintenance_action")
        existing_silver = read_iceberg_table(silver_table)
    except Exception:
        silver_table = catalog.create_table(
            "silver.maintenance_action", schema=SILVER_MAINTENANCE_ACTION_SCHEMA
        )
        existing_silver = None

    result = resolve_cdc(bronze_df, existing_silver, MAINTENANCE_ACTION_COLUMNS)

    if result.is_empty():
        logger.info("No maintenance action records to write")
        return 0

    write_iceberg_overwrite(silver_table, result)
    logger.info("Silver maintenance_action table updated: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = merge_maintenance_action()
    logger.info("Done. %d maintenance action records in Silver.", count)
