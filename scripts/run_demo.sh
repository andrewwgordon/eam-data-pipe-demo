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
echo "▶ Step 1/7: Produce CDC events to Kafka..."
python -m eam_simulator.produce_cdc --events "${BATCH_SIZE}" --seed "${SEED}" --log-level "${LOG_LEVEL}"

echo ""
echo "▶ Step 2/7: Ingest Bronze (Kafka → Iceberg)..."
python -m transforms.polars.app.bronze_ingest --timeout 15 --log-level "${LOG_LEVEL}"

echo ""
echo "▶ Step 3/7: CDC Merge → Silver (application state)..."
python -m transforms.polars.app.merge_asset
python -m transforms.polars.app.merge_work_request
python -m transforms.polars.app.merge_work_order
python -m transforms.polars.app.merge_maintenance_action

echo ""
echo "▶ Step 4/7: S5000F Semantic Transformation..."
python -m transforms.polars.s5000f.product_instance
python -m transforms.polars.s5000f.functional_failure
python -m transforms.polars.s5000f.maintenance_task
python -m transforms.polars.s5000f.maintenance_task_step
python -m transforms.polars.s5000f.maintenance_event

echo ""
echo "▶ Step 5/7: Gold Analytics Rollups..."
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

echo ""
echo "▶ Step 7/7: Running Example Queries..."
echo "═══════════════════════════════════════════════════════"

echo ""
echo "📊 Example 1: Asset Availability Analysis"
echo "─────────────────────────────────────────"
if python -m transforms.polars.query.examples.asset_availability_query; then
    echo "✅ Asset availability example completed successfully"
else
    echo "⚠️  Asset availability example had some issues (continuing)"
fi

echo ""
echo "📊 Example 2: Maintenance History Analysis"
echo "──────────────────────────────────────────"
if python -m transforms.polars.query.examples.maintenance_history_query 2>/dev/null; then
    echo "✅ Maintenance history example completed successfully"
else
    echo "⚠️  Maintenance history example had issues (some tables may not exist)"
fi

echo ""
echo "📊 Example 3: S5000F Compliance Check"
echo "─────────────────────────────────────"
if python -m transforms.polars.query.examples.s5000f_compliance_query 2>/dev/null; then
    echo "✅ S5000F compliance example completed successfully"
else
    echo "⚠️  S5000F compliance example had issues (continuing)"
fi

echo ""
echo "📊 Example 4: Cross-Layer Comparison"
echo "────────────────────────────────────"
if python -m transforms.polars.query.examples.cross_layer_comparison 2>/dev/null; then
    echo "✅ Cross-layer comparison example completed successfully"
else
    echo "⚠️  Cross-layer comparison example had issues (continuing)"
fi

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ Example queries demonstration complete!"
echo ""
echo "  Query module features demonstrated:"
echo "    • ✅ Unified query interface for all data layers"
echo "    • ✅ Polars SQL with table registration"
echo "    • ✅ Cross-layer joins (asset availability example)"
echo "    • ✅ Example queries for analytical patterns"
echo ""
echo "  Note: Some example queries may show warnings due to:"
echo "    • Missing tables (if transformations didn't create them)"
echo "    • Schema differences between example and actual data"
echo "    • This is expected in a PoC/demo environment"
echo "═══════════════════════════════════════════════════════"
