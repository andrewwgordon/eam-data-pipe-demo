"""CDC merge logic — resolves Bronze CDC events into Silver current-state tables.

Shared merge function used by all entity-specific merge scripts.
Implements idempotent, deterministic CDC resolution:
  - For each primary key, keep only the latest event (by event_ts)
  - Apply operation semantics: 'c' (create), 'u' (update), 'd' (delete)
  - Partition-replace write to Silver (safe to rerun)
"""

from __future__ import annotations

import json
import logging

import polars as pl

logger = logging.getLogger(__name__)


def resolve_cdc(
    bronze_df: pl.LazyFrame,
    existing_silver: pl.LazyFrame | None,
    entity_columns: list[str],
) -> pl.DataFrame:
    """Resolve CDC events into current-state records.

    For each primary key:
    1. Take the latest event (by event_ts)
    2. If op='d', the entity is deleted (excluded from output)
    3. If op='c' or 'u', extract the 'after' payload as current state

    Args:
        bronze_df: Bronze CDC events (LazyFrame with payload_json, event_ts, pk_id).
        existing_silver: Current Silver table (may be None on first run).
        entity_columns: Column names to extract from the CDC 'after' payload.

    Returns:
        Polars DataFrame with resolved current-state records.
    """
    # Parse CDC payloads and extract the latest event per PK
    resolved = (
        bronze_df
        .select("pk_id", "op", "event_ts", "payload_json", "source_system")
        .sort("event_ts")
        .group_by("pk_id")
        .last()  # Latest event per primary key
        .collect()
    )

    if resolved.is_empty():
        logger.info("No CDC events to resolve")
        return pl.DataFrame()

    # Separate deletes from creates/updates
    active = resolved.filter(pl.col("op") != "d")

    if active.is_empty():
        logger.info("All events are deletes — Silver table will be empty")
        return pl.DataFrame()

    # Extract entity fields from the JSON 'after' payload
    rows: list[dict] = []
    for row in active.iter_rows(named=True):
        payload = json.loads(row["payload_json"])
        after = payload.get("after", {})

        record: dict = {}
        for col in entity_columns:
            record[col] = after.get(col, None)

        # Add CDC metadata
        record["source_system"] = row["source_system"]
        record["source_id"] = row["pk_id"]
        record["last_op"] = row["op"]
        record["last_event_ts"] = row["event_ts"]
        rows.append(record)

    new_silver = pl.DataFrame(rows)

    # Merge with existing Silver: new CDC events take precedence
    if existing_silver is not None:
        existing = existing_silver.collect()
        if not existing.is_empty():
            # Get PKs that have new CDC events
            changed_pks = set(new_silver.get_column("source_id").to_list())
            # Also remove PKs that were deleted
            deleted_pks = set(
                resolved.filter(pl.col("op") == "d").get_column("pk_id").to_list()
            )
            exclude_pks = changed_pks | deleted_pks

            # Keep existing records that are NOT in the new CDC batch
            unchanged = existing.filter(~pl.col("source_id").is_in(list(exclude_pks)))
            new_silver = pl.concat([unchanged, new_silver], how="diagonal_relaxed")

    logger.info("Resolved %d current-state records", len(new_silver))
    return new_silver
