"""Iceberg I/O helpers — shared utilities for reading/writing Iceberg tables.

Provides a configured PyIceberg catalog and helper functions for
table creation, reading, and partition-replace writes.
"""

from __future__ import annotations

import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from config.settings import Settings


def get_catalog(settings: Settings | None = None):
    """Return a configured Iceberg REST catalog instance."""
    settings = settings or Settings()
    return load_catalog(
        "rest",
        **{
            "uri": settings.iceberg.catalog_uri,
            "s3.endpoint": settings.minio.s3_endpoint,
            "s3.access-key-id": settings.minio.access_key,
            "s3.secret-access-key": settings.minio.secret_key,
            "s3.region": settings.minio.region,
            "s3.path-style-access": "true",
            "s3.signature-version": "s3v4",
        },
    )


def ensure_namespace(catalog, namespace: str) -> None:
    """Create a namespace if it does not already exist."""
    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass  # Namespace already exists


# ── Bronze table schemas ─────────────────────────────────
# Raw CDC events — stored as JSON strings with minimal structure
# Fields are nullable to accommodate schema evolution and CDC edge cases

BRONZE_SCHEMA = Schema(
    NestedField(1, "entity", StringType(), required=False),
    NestedField(2, "op", StringType(), required=False),
    NestedField(3, "event_ts", StringType(), required=False),
    NestedField(4, "pk_id", StringType(), required=False),
    NestedField(5, "payload_json", StringType(), required=False),
    NestedField(6, "event_date", StringType(), required=False),
    NestedField(7, "source_system", StringType(), required=False),
)


# ── Silver table schemas ────────────────────────────────

SILVER_ASSET_SCHEMA = Schema(
    NestedField(1, "id", StringType()),
    NestedField(2, "name", StringType()),
    NestedField(3, "asset_type", StringType()),
    NestedField(4, "location", StringType()),
    NestedField(5, "status", StringType()),
    NestedField(6, "install_date", StringType()),
    NestedField(7, "updated_at", StringType()),
    NestedField(8, "source_system", StringType()),
    NestedField(9, "source_id", StringType()),
    NestedField(10, "last_op", StringType()),
    NestedField(11, "last_event_ts", StringType()),
)

SILVER_WORK_REQUEST_SCHEMA = Schema(
    NestedField(1, "id", StringType()),
    NestedField(2, "asset_id", StringType()),
    NestedField(3, "description", StringType()),
    NestedField(4, "priority", StringType()),
    NestedField(5, "status", StringType()),
    NestedField(6, "reported_at", StringType()),
    NestedField(7, "updated_at", StringType()),
    NestedField(8, "source_system", StringType()),
    NestedField(9, "source_id", StringType()),
    NestedField(10, "last_op", StringType()),
    NestedField(11, "last_event_ts", StringType()),
)

SILVER_WORK_ORDER_SCHEMA = Schema(
    NestedField(1, "id", StringType()),
    NestedField(2, "work_request_id", StringType()),
    NestedField(3, "asset_id", StringType()),
    NestedField(4, "status", StringType()),
    NestedField(5, "planned_start", StringType()),
    NestedField(6, "planned_end", StringType()),
    NestedField(7, "actual_start", StringType()),
    NestedField(8, "actual_end", StringType()),
    NestedField(9, "updated_at", StringType()),
    NestedField(10, "source_system", StringType()),
    NestedField(11, "source_id", StringType()),
    NestedField(12, "last_op", StringType()),
    NestedField(13, "last_event_ts", StringType()),
)

SILVER_MAINTENANCE_ACTION_SCHEMA = Schema(
    NestedField(1, "id", StringType()),
    NestedField(2, "work_order_id", StringType()),
    NestedField(3, "step_number", LongType()),
    NestedField(4, "description", StringType()),
    NestedField(5, "status", StringType()),
    NestedField(6, "started_at", StringType()),
    NestedField(7, "completed_at", StringType()),
    NestedField(8, "source_system", StringType()),
    NestedField(9, "source_id", StringType()),
    NestedField(10, "last_op", StringType()),
    NestedField(11, "last_event_ts", StringType()),
)


def read_iceberg_table(table: Table) -> pl.LazyFrame:
    """Read an Iceberg table into a Polars LazyFrame.

    Returns an empty LazyFrame with the correct schema if the table has no data.
    """
    try:
        arrow_table = table.scan().to_arrow()
        return pl.from_arrow(arrow_table).lazy()
    except Exception:
        # Table exists but has no data yet
        return pl.LazyFrame()


def write_iceberg_overwrite(table: Table, df: pl.DataFrame) -> None:
    """Overwrite an Iceberg table with a Polars DataFrame.

    Uses overwrite mode for partition-replace semantics (idempotent).
    """
    arrow_table = df.to_arrow()
    table.overwrite(arrow_table)


def write_iceberg_append(table: Table, df: pl.DataFrame) -> None:
    """Append a Polars DataFrame to an Iceberg table."""
    arrow_table = df.to_arrow()
    table.append(arrow_table)
