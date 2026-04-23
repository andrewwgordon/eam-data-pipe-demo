"""FunctionalFailure transform — Silver WorkRequest → Silver-S5000F FunctionalFailure.

Maps application WorkRequest entities to the S5000F FunctionalFailure concept,
representing a reported failure mode on a maintainable item.

Mapping:
    WorkRequest.id          → functional_failure_id (deterministic hash)
    WorkRequest.asset_id    → product_instance_id (deterministic hash of asset)
    WorkRequest.description → failure_description
    WorkRequest.priority    → priority
    WorkRequest.reported_at → reported_at

Usage:
    python -m transforms.polars.s5000f.functional_failure
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


def transform_functional_failure() -> int:
    """Transform Silver work requests into S5000F FunctionalFailure records.

    Returns:
        Number of FunctionalFailure records written.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "silver_s5000f")

    try:
        silver_table = catalog.load_table("silver.work_request")
        silver_df = read_iceberg_table(silver_table).collect()
    except Exception:
        logger.warning("Silver work_request table not found — nothing to transform")
        return 0

    if silver_df.is_empty():
        logger.info("Silver work_request table is empty")
        return 0

    mapped_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    for row in silver_df.iter_rows(named=True):
        rows.append({
            "functional_failure_id": s5000f_id("FunctionalFailure", row["id"]),
            "product_instance_id": s5000f_id("ProductInstance", row["asset_id"]),
            "failure_description": row.get("description", ""),
            "priority": row["priority"],
            "reported_at": row.get("reported_at", ""),
            "source_system": row["source_system"],
            "source_id": row["source_id"],
            "mapped_at": mapped_at,
        })

    result = pl.DataFrame(rows)

    try:
        s5000f_table = catalog.load_table("silver_s5000f.functional_failure")
    except Exception:
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType

        schema = Schema(
            NestedField(1, "functional_failure_id", StringType()),
            NestedField(2, "product_instance_id", StringType()),
            NestedField(3, "failure_description", StringType()),
            NestedField(4, "priority", StringType()),
            NestedField(5, "reported_at", StringType()),
            NestedField(6, "source_system", StringType()),
            NestedField(7, "source_id", StringType()),
            NestedField(8, "mapped_at", StringType()),
        )
        s5000f_table = catalog.create_table("silver_s5000f.functional_failure", schema=schema)

    write_iceberg_overwrite(s5000f_table, result)
    logger.info("Silver-S5000F functional_failure: %d records", len(result))
    return len(result)


if __name__ == "__main__":
    count = transform_functional_failure()
    logger.info("Done. %d FunctionalFailure records.", count)
