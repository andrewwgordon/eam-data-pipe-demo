"""ProductInstance transform — Silver Asset → Silver-S5000F ProductInstance.

Maps application Asset entities to the S5000F ProductInstance concept,
representing a maintainable physical item in the plant.

Mapping:
    Asset.id           → product_instance_id (deterministic hash)
    Asset.name         → name
    Asset.asset_type   → type
    Asset.location     → location
    Asset.status       → operational_status
    Asset.install_date → installation_date

Usage:
    python -m transforms.polars.s5000f.product_instance
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


def transform_product_instance() -> int:
    """Transform Silver assets into S5000F ProductInstance records.

    Returns:
        Number of ProductInstance records written.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver_s5000f")

    # Read Silver asset table
    try:
        silver_table = catalog.load_table("silver.asset")
        silver_df = read_iceberg_table(silver_table).collect()
    except Exception:
        logger.warning("Silver asset table not found — nothing to transform")
        return 0

    if silver_df.is_empty():
        logger.info("Silver asset table is empty")
        return 0

    # Map to S5000F ProductInstance
    mapped_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for row in silver_df.iter_rows(named=True):
        rows.append({
            "product_instance_id": s5000f_id("ProductInstance", row["id"]),
            "name": row["name"],
            "type": row["asset_type"],
            "location": row["location"],
            "operational_status": row["status"],
            "installation_date": row.get("install_date", ""),
            "source_system": row["source_system"],
            "source_id": row["source_id"],
            "mapped_at": mapped_at,
        })

    result = pl.DataFrame(rows)

    # Write to Silver-S5000F (overwrite — idempotent)
    try:
        s5000f_table = catalog.load_table("silver_s5000f.product_instance")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType

        schema = Schema(
            NestedField(1, "product_instance_id", StringType()),
            NestedField(2, "name", StringType()),
            NestedField(3, "type", StringType()),
            NestedField(4, "location", StringType()),
            NestedField(5, "operational_status", StringType()),
            NestedField(6, "installation_date", StringType()),
            NestedField(7, "source_system", StringType()),
            NestedField(8, "source_id", StringType()),
            NestedField(9, "mapped_at", StringType()),
        )
        s5000f_table = catalog.create_table("silver_s5000f.product_instance", schema=schema)

    write_iceberg_overwrite(s5000f_table, result)
    logger.info("Silver-S5000F product_instance: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = transform_product_instance()
    logger.info("Done. %d ProductInstance records.", count)
