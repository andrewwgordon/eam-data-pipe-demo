#!/usr/bin/env bash
# Create CDC topics in Kafka.
# Runs as a one-shot init container after Kafka is healthy.
set -euo pipefail

BOOTSTRAP="kafka:9094"
# Official Apache Kafka image puts binaries under /opt/kafka/bin/
KAFKA_BIN="/opt/kafka/bin"

TOPICS=(
  "cdc.asset"
  "cdc.work_request"
  "cdc.work_order"
  "cdc.maintenance_action"
)

echo "Creating Kafka topics on ${BOOTSTRAP}..."
for topic in "${TOPICS[@]}"; do
  echo "  → ${topic}"
  "${KAFKA_BIN}/kafka-topics.sh" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 4 \
    --replication-factor 1
done

echo "Kafka topics ready:"
"${KAFKA_BIN}/kafka-topics.sh" --bootstrap-server "${BOOTSTRAP}" --list
