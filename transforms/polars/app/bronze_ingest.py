"""Bronze ingestion — Kafka CDC events → Iceberg Bronze tables.

Consumes from all 4 CDC topics and writes raw event payloads to
Iceberg Bronze tables. Append-only, partitioned by event_date.
No semantic transformation at this layer.

Usage:
    python -m transforms.polars.app.bronze_ingest --timeout 10
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from confluent_kafka import Consumer, KafkaError

from config.settings import Settings
from transforms.polars.app.iceberg_io import (
    BRONZE_SCHEMA,
    ensure_namespace,
    get_catalog,
    write_iceberg_append,
)

import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _parse_cdc_event(raw: bytes) -> dict[str, str]:
    """Parse a raw CDC JSON payload into Bronze table columns."""
    event = json.loads(raw)
    return {
        "entity": event["entity"],
        "op": event["op"],
        "event_ts": event["event_ts"],
        "pk_id": event["pk"]["id"],
        "payload_json": raw.decode("utf-8"),
        "event_date": event["event_ts"][:10],  # ISO date portion
        "source_system": event["source"]["system"],
    }


def _bronze_table_name(entity: str) -> str:
    """Map entity type to Bronze table identifier."""
    mapping = {
        "Asset": "bronze.asset",
        "WorkRequest": "bronze.work_request",
        "WorkOrder": "bronze.work_order",
        "MaintenanceAction": "bronze.maintenance_action",
    }
    return mapping[entity]


def ingest_bronze(timeout_seconds: int = 10) -> int:
    """Consume CDC events from Kafka and write to Iceberg Bronze tables.

    Args:
        timeout_seconds: How long to poll Kafka before stopping.

    Returns:
        Total number of events ingested.
    """
    settings = Settings()
    catalog = get_catalog(settings)
    ensure_namespace(catalog, "bronze")

    # Ensure Bronze tables exist
    bronze_tables = {}
    for table_name in [
        "bronze.asset",
        "bronze.work_request",
        "bronze.work_order",
        "bronze.maintenance_action",
    ]:
        try:
            bronze_tables[table_name] = catalog.load_table(table_name)
        except Exception:
            bronze_tables[table_name] = catalog.create_table(table_name, schema=BRONZE_SCHEMA)

    # Configure Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "group.id": "bronze-ingest",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe(settings.kafka.topics)

    # Collect events by entity, then batch-write
    buffers: dict[str, list[dict[str, str]]] = {
        "bronze.asset": [],
        "bronze.work_request": [],
        "bronze.work_order": [],
        "bronze.maintenance_action": [],
    }

    total = 0
    empty_polls = 0
    max_empty = timeout_seconds  # ~1 second per empty poll

    logger.info("Consuming from Kafka topics: %s", settings.kafka.topics)

    while empty_polls < max_empty:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Consumer error: %s", msg.error())
            break

        empty_polls = 0  # Reset on successful message
        row = _parse_cdc_event(msg.value())
        table_name = _bronze_table_name(row["entity"])
        buffers[table_name].append(row)
        total += 1

    consumer.close()

    # Write buffered events to Iceberg
    for table_name, rows in buffers.items():
        if not rows:
            continue
        df = pl.DataFrame(rows)
        write_iceberg_append(bronze_tables[table_name], df)
        logger.info("Wrote %d rows to %s", len(rows), table_name)

    logger.info("Bronze ingestion complete: %d total events", total)
    return total


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Ingest CDC events from Kafka to Iceberg Bronze")
    parser.add_argument(
        "--timeout", type=int, default=10, help="Kafka poll timeout in seconds (default: 10)"
    )
    args = parser.parse_args()

    count = ingest_bronze(timeout_seconds=args.timeout)
    logger.info("Done. %d events ingested.", count)
    sys.exit(0)


if __name__ == "__main__":
    main()
