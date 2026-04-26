# eam-data-pipe-demo

Minimal proof-of-concept demonstrating a modern data-engineering pipeline for Engineering & Asset Management (EAM), with semantic transformation into ASD S5000F-aligned data structures.

## What This Demonstrates

```
CDC → Kafka → Iceberg Bronze → Silver (App State) → Silver-S5000F → Gold Analytics
```

- **Change Data Capture** from a simulated EAM system
- **Iceberg lakehouse** with Bronze / Silver / Gold layers
- **Polars** as the only batch transformation engine
- **ASD S5000F** conceptual alignment (not formal compliance)
- **Airflow** orchestration (no business logic in DAGs)

## Quick Start

### Prerequisites

- **Python 3.11+**
- **Docker & Docker Compose** (for Kafka, MinIO, Iceberg Catalog, Airflow)

### Setup & Run

```bash
# 1. Start infrastructure
docker compose up -d

# 2. Install Python dependencies (includes PyArrow, PyIceberg, Polars, Kafka client)
pip install -e '.[dev]'

# 3. Run the full demo pipeline
bash scripts/run_demo.sh

# 4. Run tests
pytest
```

**Expected output from `bash scripts/run_demo.sh`:**
- Step 1/6: Produces 100 CDC events to Kafka
- Step 2/6: Ingests ~120 rows into Bronze tables (Bronze has all entity changes)
- Step 3/6: Merges CDC into Silver (current state): ~20 assets, ~12 work requests, etc.
- Step 4/6: Transforms to S5000F: ProductInstance, FunctionalFailure, MaintenanceTask, etc.
- Step 5/6: Gold analytics: asset availability, work order backlog, maintenance history, MTBF
- All 4 layers (Bronze, Silver, S5000F, Gold) populated in Iceberg

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full architecture overview.

### Pipeline Flow

| Layer | Purpose | Technology |
|-------|---------|------------|
| **Simulator** | Generate CDC events | Python + Faker |
| **Kafka** | Event transport | Bitnami Kafka (KRaft) |
| **Bronze** | Raw CDC storage | Iceberg (append-only) |
| **Silver** | Application current state | Polars CDC merge → Iceberg |
| **Silver-S5000F** | Standardised model | Polars semantic mapping → Iceberg |
| **Gold** | Analytics & reporting | Polars aggregation → Iceberg |

### S5000F Mapping

| S5000F Concept | Source Entity |
|----------------|---------------|
| ProductInstance | Asset |
| FunctionalFailure | WorkRequest |
| MaintenanceTask | WorkOrder |
| MaintenanceTaskStep | MaintenanceAction |
| MaintenanceEvent | WorkOrder + Actions |

See [docs/s5000f_mapping.md](docs/s5000f_mapping.md) for detailed field mappings.

## Project Structure

```
├── eam_simulator/          # Simulated EAM system + CDC producer
│   ├── entities/           # Entity dataclasses (Asset, WorkOrder, etc.)
│   ├── event_generator.py  # Lifecycle simulation engine
│   └── produce_cdc.py      # Kafka CDC producer
├── transforms/polars/
│   ├── app/                # Bronze ingestion + CDC merge to Silver
│   ├── s5000f/             # Semantic transformation to S5000F
│   ├── gold/               # Analytics rollups
│   └── query/              # Iceberg query module (Phase 6)
├── airflow/dags/           # Orchestration DAGs (no business logic)
├── config/                 # Centralised settings (env-var driven)
├── docs/                   # Architecture & mapping documentation
├── scripts/                # Infrastructure init + demo scripts
└── tests/                  # Unit and integration tests
```

## Infrastructure (Docker Compose)

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | CDC event transport |
| MinIO | 9000 / 9001 | S3-compatible object storage |
| Iceberg REST | 8181 | Table catalog |
| Airflow | 8080 | Pipeline orchestration |

## Iceberg Query Module (Phase 6)

The query module provides a unified interface for analytical exploration across all data layers using Polars SQL.

### Key Features
- **Unified Query Interface**: Single function for all query types
- **Cross-Layer Joins**: Join tables across Silver, Silver-S5000F, and Gold layers
- **Automatic Partition Filtering**: Date-based partition pruning
- **Example Queries**: Pre-built queries for common analytical patterns

### Usage Examples
```python
from transforms.polars.query import query_iceberg

# Query a single table
df = query_iceberg("silver.asset", date_filter="2024-01-01", limit=10)

# Custom SQL query with join
sql = """
SELECT a.id, a.name, wo.status
FROM silver_asset a
JOIN silver_work_order wo ON a.id = wo.asset_id
WHERE wo.status = 'OPEN'
"""
df = query_iceberg(sql=sql, date_filter="2024-01-01")
```

### Running Examples
```bash
# Run example queries
python -m transforms.polars.query.examples.asset_availability_query
python -m transforms.polars.query.examples.maintenance_history_query
python -m transforms.polars.query.examples.s5000f_compliance_query
python -m transforms.polars.query.examples.cross_layer_comparison
```

See [docs/query_module.md](docs/query_module.md) for complete documentation.

## Technology Constraints

- ✅ **Polars only** — no Spark, DuckDB, or Pandas
- ✅ **Kafka is transport** — append-only, no replay logic
- ✅ **Airflow orchestrates** — no business logic in DAGs
- ✅ **Bounded computation** — no unbounded table scans
- ✅ **Explicit mappings** — no hidden abstractions

## Troubleshooting

### Issue: "Could not initialize FileIO: PyArrowFileIO"

**Cause:** Missing `pyarrow` dependency.

**Fix:** Ensure it's installed:
```bash
pip install -e '.[dev]'  # Installs pyarrow as a dependency
# OR
pip install pyarrow>=17.0.0
```

### Issue: Kafka consumer never gets messages, times out after 15 seconds

**Cause:** KRaft Kafka broker misconfigured. The `__consumer_offsets` internal topic cannot auto-create due to wrong replication factor.

**Fix:** Kafka must be initialized with:
```yaml
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
```

If already running, rebuild:
```bash
docker compose down kafka kafka-init -v
docker volume rm eam-data-pipe-demo_kafka-data  # Clean stale KRaft metadata
docker compose up -d kafka kafka-init
sleep 20  # Wait for Kafka to be healthy
```

### Issue: Iceberg schema mismatch errors ("required string" vs "optional string")

**Cause:** Polars DataFrames infer all columns as nullable by default, but Iceberg schemas had strict `required=True` constraints.

**Fix:** All Iceberg table schemas now use nullable fields (no `required=True`). If you have existing tables with old schemas, drop and recreate them:
```bash
python -c "
from config.settings import Settings
from transforms.polars.app.iceberg_io import get_catalog

catalog = get_catalog()
for table_id in ['bronze.asset', 'silver.asset', 'silver_s5000f.product_instance', 'gold.asset_availability']:
    try:
        catalog.drop_table(table_id)
        print(f'Dropped {table_id}')
    except: pass
"
# Then re-run the pipeline
```

### Issue: Docker containers won't start or keep crashing

**Fix:** Ensure enough disk space and clean up old volumes:
```bash
docker compose down -v              # Remove all volumes
docker compose up -d                # Start fresh
sleep 30                            # Wait for health checks
```

## License

MIT
