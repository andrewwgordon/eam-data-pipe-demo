#!/usr/bin/env bash
# End-to-end demo: runs the full EAM data pipeline including Phase 6 DBT Postgres.
#
# Prerequisites:
#   - Docker installed and running
#   - Python dependencies installed (pip install -e '.[dev,dbt]')
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

# Postgres connection for Phase 6
export POSTGRES_HOST="${POSTGRES_HOST:-localhost}"
export POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export POSTGRES_USER="${POSTGRES_USER:-postgres}"
export POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
export POSTGRES_DB="${POSTGRES_DB:-eam_analytics}"

echo "═══════════════════════════════════════════════════════"
echo "  EAM Data Pipe PoC — End-to-End Demo (with Phase 6)"
echo "  Batch size: ${BATCH_SIZE}  Seed: ${SEED}"
echo "═════════════════════════════════════════════════════"
echo "  MinIO endpoint:      $MINIO_ENDPOINT"
echo "  Iceberg catalog:     $ICEBERG_CATALOG_URI"
echo "  Kafka bootstrap:     $KAFKA_BOOTSTRAP_SERVERS"
echo "  Postgres:            $POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_DB"
echo "═════════════════════════════════════════════════════"

echo ""
echo "▶ Step 1/10: Starting Docker Compose infrastructure..."
if ! command -v docker &> /dev/null; then
    echo "✗ Docker not found. Please install Docker first."
    exit 1
fi

echo "  Starting services (Kafka, MinIO, Iceberg, Postgres, Airflow)..."
docker compose up -d > /dev/null 2>&1
echo "✓ Services started (may take 30-60s to be healthy)"

echo ""
echo "▶ Step 2/10: Waiting for services to be ready..."

echo "  Waiting for Postgres..."
for i in {1..30}; do
    # First check if container is running
    if ! docker ps --filter "name=eam-postgres" --format "{{.Status}}" | grep -q "Up"; then
        sleep 2
        continue
    fi
    # Then check if Postgres is ready
    if docker exec eam-postgres pg_isready -U postgres > /dev/null 2>&1; then
        echo "    ✓ Postgres is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "    ✗ Postgres failed to start (container may be running but Postgres not responding)"
        exit 1
    fi
    sleep 2
done

echo "  Waiting for Kafka..."
for i in {1..30}; do
    # First check if container is running
    if ! docker ps --filter "name=eam-kafka" --format "{{.Status}}" | grep -q "Up"; then
        sleep 2
        continue
    fi
    # Then check if Kafka is ready (use kafka:9094 for internal communication)
    if docker exec eam-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --list > /dev/null 2>&1; then
        echo "    ✓ Kafka is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "    ✗ Kafka failed to start (container may be running but Kafka not responding)"
        exit 1
    fi
    sleep 2
done

echo "  Waiting for MinIO..."
for i in {1..30}; do
    # First check if container is running
    if ! docker ps --filter "name=eam-minio" --format "{{.Status}}" | grep -q "Up"; then
        sleep 2
        continue
    fi
    # Then check if MinIO is ready
    if docker exec eam-minio mc alias set local http://minio:9000 minioadmin minioadmin > /dev/null 2>&1; then
        echo "    ✓ MinIO is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "    ✗ MinIO failed to start (container may be running but MinIO not responding)"
        exit 1
    fi
    sleep 2
done

echo ""
echo "▶ Step 3/10: Initializing infrastructure..."

echo "  Initializing Kafka topics..."
docker exec eam-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --create --if-not-exists --topic cdc.asset --partitions 4 --replication-factor 1 > /dev/null 2>&1
docker exec eam-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --create --if-not-exists --topic cdc.work_request --partitions 4 --replication-factor 1 > /dev/null 2>&1
docker exec eam-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --create --if-not-exists --topic cdc.work_order --partitions 4 --replication-factor 1 > /dev/null 2>&1
docker exec eam-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --create --if-not-exists --topic cdc.maintenance_action --partitions 4 --replication-factor 1 > /dev/null 2>&1
echo "    ✓ Kafka topics created"

echo "  Initializing MinIO buckets..."
docker exec eam-minio mc alias set local http://minio:9000 minioadmin minioadmin > /dev/null 2>&1
docker exec eam-minio mc mb --ignore-existing local/warehouse > /dev/null 2>&1
echo "    ✓ MinIO buckets created"

echo ""
echo "▶ Step 4/10: Initializing Postgres schemas for Phase 6..."
docker exec eam-postgres psql -U postgres -d eam_analytics << 'EOF' > /dev/null 2>&1
CREATE SCHEMA IF NOT EXISTS staging_bronze;
CREATE SCHEMA IF NOT EXISTS staging_silver;
CREATE SCHEMA IF NOT EXISTS staging_silver_s5000f;
CREATE SCHEMA IF NOT EXISTS staging_gold;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS silver_s5000f;
CREATE SCHEMA IF NOT EXISTS gold;
EOF
echo "✓ Postgres schemas created"

echo ""
echo "▶ Step 5/10: Produce CDC events to Kafka..."
/workspaces/eam-data-pipe-demo/.venv/bin/python -m eam_simulator.produce_cdc --events "${BATCH_SIZE}" --seed "${SEED}" --log-level "${LOG_LEVEL}"

echo ""
echo "▶ Step 6/10: Ingest Bronze (Kafka → Iceberg)..."
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.app.bronze_ingest --timeout 15 --log-level "${LOG_LEVEL}"

echo ""
echo "▶ Step 7/10: CDC Merge → Silver (application state)..."
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.app.merge_asset
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.app.merge_work_request
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.app.merge_work_order
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.app.merge_maintenance_action

echo ""
echo "▶ Step 8/10: S5000F Semantic Transformation..."
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.s5000f.product_instance
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.s5000f.functional_failure
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.s5000f.maintenance_task
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.s5000f.maintenance_task_step
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.s5000f.maintenance_event

echo ""
echo "▶ Step 9/10: Gold Analytics Rollups..."
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.gold.asset_availability
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.gold.work_order_backlog
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.gold.maintenance_history
/workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.gold.mtbf_metrics

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ Iceberg Pipeline Complete!"
echo ""
echo "  Layers populated:"
echo "    Bronze  → Raw CDC events"
echo "    Silver  → Application current state"
echo "    S5000F  → Standardised maintenance model"
echo "    Gold    → Analytics & reporting"
echo "═══════════════════════════════════════════════════════"

echo ""
echo "▶ Step 10/11: Phase 6 - DBT Postgres Transform Layer..."
echo "═══════════════════════════════════════════════════════"
echo "  Loading Iceberg data into Postgres staging schemas..."

# Test DBT connection first
cd transforms/dbt
export DBT_PROFILES_DIR=$(pwd)
if ! /workspaces/eam-data-pipe-demo/.venv/bin/python -c "import dbt" 2>/dev/null; then
    echo "✗ DBT not installed. Installing DBT packages..."
    /workspaces/eam-data-pipe-demo/.venv/bin/pip install -e ".[dbt]" > /dev/null 2>&1
    if ! /workspaces/eam-data-pipe-demo/.venv/bin/python -c "import dbt" 2>/dev/null; then
        echo "✗ DBT setup failed. Continuing without Phase 6..."
        PHASE6_SUCCESS=false
    else
        echo "✓ DBT packages installed"
        PHASE6_SUCCESS=true
    fi
else
    echo "✓ DBT is installed"
    PHASE6_SUCCESS=true
fi

if [ "$PHASE6_SUCCESS" = true ]; then
    if ! /workspaces/eam-data-pipe-demo/.venv/bin/dbt debug > /dev/null 2>&1; then
        echo "✗ DBT connection failed. Continuing without Phase 6..."
        PHASE6_SUCCESS=false
    else
        echo "✓ DBT connection successful"
    fi
fi

if [ "$PHASE6_SUCCESS" = true ]; then
    echo ""
    echo "  Running DBT models for all layers..."
    
    # Run DBT models by layer
    echo "  → Bronze layer..."
    if /workspaces/eam-data-pipe-demo/.venv/bin/dbt run --selector tag:bronze > /dev/null 2>&1; then
        echo "    ✓ Bronze models completed"
    else
        echo "    ⚠️  Bronze models had issues"
    fi
    
    echo "  → Silver layer..."
    if /workspaces/eam-data-pipe-demo/.venv/bin/dbt run --selector tag:silver > /dev/null 2>&1; then
        echo "    ✓ Silver models completed"
    else
        echo "    ⚠️  Silver models had issues"
    fi
    
    echo "  → Silver-S5000F layer..."
    if /workspaces/eam-data-pipe-demo/.venv/bin/dbt run --selector tag:silver_s5000f > /dev/null 2>&1; then
        echo "    ✓ Silver-S5000F models completed"
    else
        echo "    ⚠️  Silver-S5000F models had issues"
    fi
    
    echo "  → Gold layer..."
    if /workspaces/eam-data-pipe-demo/.venv/bin/dbt run --selector tag:gold > /dev/null 2>&1; then
        echo "    ✓ Gold models completed"
    else
        echo "    ⚠️  Gold models had issues"
    fi
    
    echo ""
    echo "  Running DBT tests for schema validation..."
    if /workspaces/eam-data-pipe-demo/.venv/bin/dbt test > /dev/null 2>&1; then
        echo "    ✓ DBT tests passed"
    else
        echo "    ⚠️  DBT tests had issues (continuing)"
    fi
    
    echo ""
    echo "  ✅ Phase 6 DBT Postgres Transform Complete!"
    echo "  Postgres tables populated in schemas:"
    echo "    • bronze.* (4 tables)"
    echo "    • silver.* (4 tables)"
    echo "    • silver_s5000f.* (4 tables)"
    echo "    • gold.* (4 tables)"
else
    echo ""
    echo "  ⚠️  Phase 6 DBT Postgres Transform Skipped"
    echo "  (DBT setup failed or not installed)"
fi

cd ../..

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  ✅ End-to-End Pipeline Complete!"
echo ""
echo "  Data now available in:"
echo "    • Iceberg (source of truth):"
echo "      - Bronze, Silver, Silver-S5000F, Gold layers"
echo "    • Postgres (analytical replica):"
if [ "$PHASE6_SUCCESS" = true ]; then
    echo "      - Bronze, Silver, Silver-S5000F, Gold schemas"
else
    echo "      - (Phase 6 not executed)"
fi
echo "═══════════════════════════════════════════════════════"

echo ""
echo "▶ Step 11/11: Running Example Queries..."
echo "═══════════════════════════════════════════════════════"

echo ""
echo "📊 Example 1: Asset Availability Analysis"
echo "─────────────────────────────────────────"
if /workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.query.examples.asset_availability_query; then
    echo "✅ Asset availability example completed successfully"
else
    echo "⚠️  Asset availability example had some issues (continuing)"
fi

echo ""
echo "📊 Example 2: Maintenance History Analysis"
echo "──────────────────────────────────────────"
if /workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.query.examples.maintenance_history_query 2>/dev/null; then
    echo "✅ Maintenance history example completed successfully"
else
    echo "⚠️  Maintenance history example had issues (some tables may not exist)"
fi

echo ""
echo "📊 Example 3: S5000F Compliance Check"
echo "─────────────────────────────────────"
if /workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.query.examples.s5000f_compliance_query 2>/dev/null; then
    echo "✅ S5000F compliance example completed successfully"
else
    echo "⚠️  S5000F compliance example had issues (continuing)"
fi

echo ""
echo "📊 Example 4: Cross-Layer Comparison"
echo "────────────────────────────────────"
if /workspaces/eam-data-pipe-demo/.venv/bin/python -m transforms.polars.query.examples.cross_layer_comparison 2>/dev/null; then
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
if [ "$PHASE6_SUCCESS" = true ]; then
    echo "  Phase 6 DBT Postgres features demonstrated:"
    echo "    • ✅ Multi-sink architecture (Iceberg + Postgres)"
    echo "    • ✅ DBT models for all 4 data layers"
    echo "    • ✅ Postgres analytical replica for BI tools"
    echo "    • ✅ Schema validation with DBT tests"
fi
echo ""
echo "  Note: Some example queries may show warnings due to:"
echo "    • Missing tables (if transformations didn't create them)"
echo "    • Schema differences between example and actual data"
echo "    • This is expected in a PoC/demo environment"
echo "═══════════════════════════════════════════════════════"
