"""Merge WorkRequest — Bronze CDC → Silver current-state table.

Resolves WorkRequest CDC events into a single-row-per-request Silver table.

Usage:
    python -m transforms.polars.app.merge_work_request
"""

from __future__ import annotations

import logging

from config.settings import Settings
from transforms.polars.app.cdc_merge import resolve_cdc
from transforms.polars.app.iceberg_io import (
    SILVER_WORK_REQUEST_SCHEMA,
    ensure_namespace,
    get_catalog,
    read_iceberg_table,
    write_iceberg_overwrite,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WORK_REQUEST_COLUMNS = [
    "id", "asset_id", "description", "priority", "status", "reported_at", "updated_at",
]


def merge_work_request() -> int:
    """Resolve WorkRequest CDC events from Bronze into Silver.

    Returns:
        Number of records in the resulting Silver table.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver")

    try:
        bronze_table = catalog.load_table("bronze.work_request")
        bronze_df = read_iceberg_table(bronze_table)
    except Exception:
        logger.warning("Bronze work_request table not found — nothing to merge")
        return 0

    try:
        silver_table = catalog.load_table("silver.work_request")
        existing_silver = read_iceberg_table(silver_table)
    except Exception:
        silver_table = catalog.create_table(
            "silver.work_request", schema=SILVER_WORK_REQUEST_SCHEMA
        )
        existing_silver = None

    result = resolve_cdc(bronze_df, existing_silver, WORK_REQUEST_COLUMNS)

    if result.is_empty():
        logger.info("No work request records to write")
        return 0

    write_iceberg_overwrite(silver_table, result)
    logger.info("Silver work_request table updated: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = merge_work_request()
    logger.info("Done. %d work request records in Silver.", count)
