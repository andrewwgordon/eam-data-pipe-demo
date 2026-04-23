"""Merge Asset — Bronze CDC → Silver current-state table.

Resolves Asset CDC events into a single-row-per-asset Silver table.
Uses partition-replace semantics (idempotent, safe to rerun).

Usage:
    python -m transforms.polars.app.merge_asset
"""

from __future__ import annotations

import logging

from config.settings import Settings
from transforms.polars.app.cdc_merge import resolve_cdc
from transforms.polars.app.iceberg_io import (
    SILVER_ASSET_SCHEMA,
    ensure_namespace,
    get_catalog,
    read_iceberg_table,
    write_iceberg_overwrite,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Columns to extract from the CDC 'after' payload
ASSET_COLUMNS = ["id", "name", "asset_type", "location", "status", "install_date", "updated_at"]


def merge_asset() -> int:
    """Resolve Asset CDC events from Bronze into Silver.

    Returns:
        Number of records in the resulting Silver table.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver")

    # Load Bronze source
    try:
        bronze_table = catalog.load_table("bronze.asset")
        bronze_df = read_iceberg_table(bronze_table)
    except Exception:
        logger.warning("Bronze asset table not found — nothing to merge")
        return 0

    # Load existing Silver (or None on first run)
    try:
        silver_table = catalog.load_table("silver.asset")
        existing_silver = read_iceberg_table(silver_table)
    except Exception:
        silver_table = catalog.create_table("silver.asset", schema=SILVER_ASSET_SCHEMA)
        existing_silver = None

    # Resolve CDC
    result = resolve_cdc(bronze_df, existing_silver, ASSET_COLUMNS)

    if result.is_empty():
        logger.info("No asset records to write")
        return 0

    # Partition-replace write (idempotent)
    write_iceberg_overwrite(silver_table, result)
    logger.info("Silver asset table updated: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = merge_asset()
    logger.info("Done. %d asset records in Silver.", count)
