"""Kafka CDC producer — sends simulated EAM events to Kafka topics.

Routes each CDC event to the correct topic based on entity type,
partitioned by primary key for ordering guarantees.

Usage:
    python -m eam_simulator.produce_cdc --events 100
    python -m eam_simulator.produce_cdc --events 50 --seed 123
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from confluent_kafka import KafkaError, Producer

from config.settings import Settings
from eam_simulator.event_generator import EAMSimulator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _delivery_callback(err: KafkaError | None, msg: object) -> None:
    """Log delivery result for each produced message."""
    if err is not None:
        logger.error("Delivery failed: %s", err)
    else:
        logger.debug("Delivered to %s [%s]", msg.topic(), msg.partition())  # type: ignore[attr-defined]


def produce_events(n_events: int, seed: int | None = None) -> int:
    """Generate and produce CDC events to Kafka.

    Args:
        n_events: Number of events to generate.
        seed: Random seed for reproducibility.

    Returns:
        Number of events successfully queued for delivery.
    """
    settings = Settings()
    effective_seed = seed if seed is not None else settings.simulator.random_seed

    # Initialise simulator
    simulator = EAMSimulator(seed=effective_seed)
    events = simulator.generate_batch(n_events)
    logger.info("Generated %d CDC events (seed=%d)", len(events), effective_seed)

    # Initialise Kafka producer
    producer = Producer({
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "client.id": "eam-simulator",
    })

    # Produce events to appropriate topics
    produced = 0
    for event in events:
        entity = event["entity"]
        topic = settings.kafka.topic_for_entity(entity)
        key = event["pk"]["id"]
        value = json.dumps(event, default=str)

        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=_delivery_callback,
        )
        produced += 1

        # Periodic flush to avoid buffer overflow
        if produced % 50 == 0:
            producer.flush(timeout=10)

    # Final flush
    remaining = producer.flush(timeout=30)
    if remaining > 0:
        logger.warning("%d messages still in queue after final flush", remaining)

    logger.info(
        "Produced %d events across topics: %s",
        produced,
        ", ".join(settings.kafka.topics),
    )
    return produced


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Produce simulated EAM CDC events to Kafka")
    parser.add_argument(
        "--events", type=int, default=100, help="Number of events to generate (default: 100)"
    )
    parser.add_argument(
        "--seed", type=int, default=None, help="Random seed for reproducibility"
    )
    args = parser.parse_args()

    count = produce_events(n_events=args.events, seed=args.seed)
    logger.info("Done. %d events produced.", count)
    sys.exit(0)


if __name__ == "__main__":
    main()
