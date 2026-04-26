#!/usr/bin/env bash
# End-to-end demo: runs the full EAM data pipeline.
#
# Prerequisites:
#   - Docker Compose stack running (docker compose up -d)
#   - Python dependencies installed (pip install -e '.[dev]')
#
# Usage:
#   bash scripts/run_demo.sh [BATCH_SIZE]
set -euo pipefail

BATCH_SIZE="${1:-100}"
SEED=42
LOG_LEVEL="DEBUG"

# Set S3 credentials for object-store library (prevents AWS metadata service fallback)
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-minioadmin}"

# Override endpoints for local development (host machine accessing Docker services)
export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export MINIO_ENDPOINT="${MINIO_ENDPOINT:-localhost:9000}"
export ICEBERG_CATALOG_URI="${ICEBERG_CATALOG_URI:-http://localhost:8181}"

echo "═══════════════════════════════════════════════════════"
echo "  EAM Data Pipe PoC — End-to-End Demo"
echo "  Batch size: ${BATCH_SIZE}  Seed: ${SEED}"
echo "═════════════════════════════════════════════════════"
echo "  MinIO endpoint:      $MINIO_ENDPOINT"
echo "  Iceberg catalog:     $ICEBERG_CATALOG_URI"
echo "  Kafka bootstrap:     $KAFKA_BOOTSTRAP_SERVERS"
echo "═════════════════════════════════════════════════════"

echo ""
echo "▶ Step 1/6: Produce CDC events to Kafka..."
python -m eam_simulator.produce_cdc --events "${BATCH_SIZE}" --seed "${SEED}" --log-level "${LOG_LEVEL}"

echo ""
echo "▶ Step 2/6: Ingest Bronze (Kafka → Iceberg)..."
python -m transforms.polars.app.bronze_ingest --timeout 15 --log-level "${LOG_LEVEL}"

echo ""
echo "▶ Step 3/6: CDC Merge → Silver (application state)..."
python -m transforms.polars.app.merge_asset
python -m transforms.polars.app.merge_work_request
python -m transforms.polars.app.merge_work_order
python -m transforms.polars.app.merge_maintenance_action

echo ""
echo "▶ Step 4/6: S5000F Semantic Transformation..."
python -m transforms.polars.s5000f.product_instance
python -m transforms.polars.s5000f.functional_failure
python -m transforms.polars.s5000f.maintenance_task
python -m transforms.polars.s5000f.maintenance_task_step
python -m transforms.polars.s5000f.maintenance_event

echo ""
echo "▶ Step 5/6: Gold Analytics Rollups..."
python -m transforms.polars.gold.asset_availability
python -m transforms.polars.gold.work_order_backlog
python -m transforms.polars.gold.maintenance_history
python -m transforms.polars.gold.mtbf_metrics

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ Pipeline complete!"
echo ""
echo "  Layers populated:"
echo "    Bronze  → Raw CDC events"
echo "    Silver  → Application current state"
echo "    S5000F  → Standardised maintenance model"
echo "    Gold    → Analytics & reporting"
echo "═══════════════════════════════════════════════════════"
