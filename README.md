# eam-data-pipe-demo

Minimal proof-of-concept demonstrating a modern data-engineering pipeline for Engineering & Asset Management (EAM), with semantic transformation into ASD S5000F-aligned data structures.

## What This Demonstrates

```
CDC → Kafka → Iceberg Bronze → Silver (App State) → Silver-S5000F → Gold Analytics → Postgres (DBT)
```

- **Change Data Capture** from a simulated EAM system
- **Iceberg lakehouse** with Bronze / Silver / Gold layers
- **Polars** as the only batch transformation engine
- **ASD S5000F** conceptual alignment (not formal compliance)
- **Airflow** orchestration (no business logic in DAGs)
- **Data Contracts** defined in ODCS YAML for Bronze, Silver, Silver-S5000F, and Gold datasets
- **Hybrid Architecture**: Iceberg (source of truth) + Postgres (analytical replica via DBT)

## Quick Start

### Prerequisites

- **Python 3.11+**
- **Docker & Docker Compose** (for Kafka, MinIO, Iceberg Catalog, Airflow)

### Setup & Run

```bash
# 1. Install Python dependencies (includes PyArrow, PyIceberg, Polars, Kafka client, DBT)
pip install -e '.[dev,dbt]'

# 2. Run the full demo pipeline (starts infrastructure, runs pipeline, includes Phase 6 DBT Postgres transform)
bash scripts/run_demo.sh [BATCH_SIZE]

# 3. Run tests
pytest
```

**Expected output from `bash scripts/run_demo.sh`:**
- Step 1/11: Starts Docker infrastructure (Kafka, MinIO, Iceberg, Postgres, Airflow)
- Step 2/11: Waits for services to be ready with health checks
- Step 3/11: Initializes infrastructure (Kafka topics, MinIO buckets, Postgres schemas)
- Step 4/11: Initializes Postgres schemas for Phase 6 DBT transform
- Step 5/11: Produces CDC events to Kafka (default: 100 events)
- Step 6/11: Ingests Bronze layer (Kafka → Iceberg)
- Step 7/11: CDC Merge → Silver (application state)
- Step 8/11: S5000F Semantic Transformation
- Step 9/11: Gold Analytics Rollups
- Step 10/11: Phase 6 - DBT Postgres Transform (Iceberg → Postgres replication)
- Step 11/11: Runs example queries demonstrating hybrid architecture

**Data now available in:**
- **Iceberg** (source of truth): Bronze, Silver, Silver-S5000F, Gold layers
- **Postgres** (analytical replica): Bronze, Silver, Silver-S5000F, Gold schemas (via DBT)

## Architecture

See [docs/architecture.md](docs/architecture.md) for the full architecture overview, including the new ODCS data contract layer.

### Pipeline Flow

| Layer | Purpose | Technology | Phase 6 (Postgres) |
|-------|---------|------------|-------------------|
| **Simulator** | Generate CDC events | Python + Faker | - |
| **Kafka** | Event transport | Apache Kafka (KRaft) | - |
| **Bronze** | Raw CDC storage | Iceberg (append-only) | Postgres via DBT |
| **Silver** | Application current state | Polars CDC merge → Iceberg | Postgres via DBT |
| **Silver-S5000F** | Standardised model | Polars semantic mapping → Iceberg | Postgres via DBT |
| **Gold** | Analytics & reporting | Polars aggregation → Iceberg | Postgres via DBT |

**Phase 6 Architecture**: Hybrid design with Iceberg as source of truth and Postgres as analytical replica for BI tools.

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
├── transforms/
│   ├── polars/             # Primary transformation engine (Polars)
│   │   ├── app/            # Bronze ingestion + CDC merge to Silver
│   │   ├── s5000f/         # Semantic transformation to S5000F
│   │   ├── gold/           # Analytics rollups
│   │   └── query/          # Iceberg query module
│   └── dbt/                # Phase 6: DBT Postgres transform layer
│       ├── models/         # 16 DBT models across 4 layers
│       ├── macros/         # DBT macros for lineage preservation
│       ├── dbt_project.yml # DBT configuration
│       └── profiles.yml    # Postgres connection profiles
├── airflow/dags/           # Orchestration DAGs (no business logic)
├── config/                 # Centralised settings (env-var driven)
├── docs/                   # Architecture, mapping, and documentation
│   ├── data_contracts/     # Open Data Contract Standard YAML dataset contracts
│   ├── data_products/      # Open Data Product Standard definitions
│   └── PHASE_6_*          # Phase 6 implementation documentation
├── scripts/                # Infrastructure init + demo scripts
└── tests/                  # Unit and integration tests
```

## Infrastructure (Docker Compose)

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | CDC event transport |
| MinIO | 9000 / 9001 | S3-compatible object storage |
| Iceberg REST | 8181 | Table catalog |
| Postgres | 5432 | Phase 6: Analytical database for DBT transforms |
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

## Phase 6: DBT Postgres Transform Layer

Phase 6 adds a DBT-based Postgres analytical replica to complement the existing Polars-based Iceberg pipeline. This creates a hybrid architecture where Iceberg remains the source of truth, while Postgres serves as a high-performance analytical replica for BI tools and ad-hoc queries.

### Architecture Overview
- **Iceberg**: Source of truth, immutable snapshots, partition-aware storage
- **Postgres**: Analytical replica, high-performance queries, BI tool compatibility  
- **Polars**: Primary transformation engine (batch processing)
- **DBT**: Secondary transformation for Postgres modeling and lineage

### DBT Project Structure
```
transforms/dbt/
├── dbt_project.yml          # DBT configuration
├── profiles.yml             # Postgres connection profiles
├── models/
│   ├── staging/             # Iceberg source extraction models
│   ├── bronze/              # 4 models: Raw CDC replicas
│   ├── silver/              # 4 models: Application state replicas
│   ├── silver_s5000f/       # 4 models: Standardized maintenance models
│   └── gold/                # 4 models: Analytics and reporting
├── macros/
│   └── get_iceberg_snapshot_id.sql  # Lineage preservation macro
└── README.md                # Comprehensive documentation
```

### Key Features
1. **Hybrid Architecture**: Iceberg (source of truth) + Postgres (analytical replica)
2. **Complete Data Replication**: All 4 data layers replicated to Postgres
3. **Lineage Preservation**: `source_system`, `source_id`, `dbt_created_at`, `dbt_updated_at` columns
4. **Schema Validation**: DBT tests for data quality and schema consistency
5. **BI Tool Ready**: Postgres enables direct connection from Tableau, Power BI, Metabase, etc.

### Airflow Orchestration
- **DAG**: `dbt_postgres_transform_dag.py`
- **Tasks**: 16 extract tasks + 4 DBT transform tasks + validation
- **Execution**: Runs after `gold_rollups_dag.py` in pipeline sequence
- **Error Handling**: Graceful degradation if DBT setup fails

### Usage
```bash
# Manual DBT execution
cd transforms/dbt
export DBT_PROFILES_DIR=$(pwd)
dbt debug                    # Test connection
dbt run --selector tag:bronze
dbt run --selector tag:silver
dbt run --selector tag:silver_s5000f
dbt run --selector tag:gold
dbt test                     # Run schema validation

# Check Postgres tables
docker exec eam-postgres psql -U postgres -d eam_analytics -c "\dt bronze.*"
docker exec eam-postgres psql -U postgres -d eam_analytics -c "\dt silver.*"
```

See [docs/PHASE_6_IMPLEMENTATION.md](docs/PHASE_6_IMPLEMENTATION.md) and [docs/PHASE_6_SUMMARY.md](docs/PHASE_6_SUMMARY.md) for complete implementation details.

## Technology Constraints

- ✅ **Polars primary** — main batch transformation engine (no Spark, DuckDB, or Pandas)
- ✅ **DBT secondary** — complementary transformation for Postgres analytical replica
- ✅ **Kafka is transport** — append-only, no replay logic
- ✅ **Airflow orchestrates** — no business logic in DAGs
- ✅ **Bounded computation** — no unbounded table scans
- ✅ **Explicit mappings** — no hidden abstractions
- ✅ **Hybrid architecture** — Iceberg source of truth + Postgres analytical replica

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

### Issue: Phase 6 DBT connection fails

**Cause:** DBT not installed or Postgres not accessible.

**Fix:** 
```bash
# Install DBT dependencies
pip install -e ".[dbt]"

# Check Postgres is running
docker ps --filter "name=eam-postgres"

# Test DBT connection
cd transforms/dbt
export DBT_PROFILES_DIR=$(pwd)
dbt debug
```

### Issue: DBT project validation errors

**Cause:** Configuration issues in `dbt_project.yml` or `profiles.yml`.

**Fix:**
```bash
# Check DBT configuration
cd transforms/dbt
dbt debug --config-dir

# Common fixes:
# 1. Ensure project name has no hyphens (use underscores)
# 2. Remove deprecated `data-paths` configuration
# 3. Verify Postgres connection details in profiles.yml
```

### Issue: Kafka waiting logic fails in demo script

**Cause:** Kafka container not ready or command path incorrect.

**Fix:** The script has been updated with robust health checks:
- Checks container status before executing commands
- Uses full path `/opt/kafka/bin/kafka-topics.sh` (not relying on PATH)
- Uses `kafka:9094` for internal container communication
- Includes descriptive error messages

If issues persist, manually test:
```bash
# Check container status
docker ps --filter "name=eam-kafka" --format "{{.Status}}"

# Test Kafka connection
docker exec eam-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --list
```

## Updated Demo Script (`scripts/run_demo.sh`)

The demo script has been significantly enhanced to provide robust end-to-end execution:

### Key Improvements
1. **Service Health Checks**: Waits for all services (Postgres, Kafka, MinIO) with container status verification
2. **Infrastructure Initialization**: Creates Kafka topics, MinIO buckets, and Postgres schemas
3. **Phase 6 Integration**: Includes DBT Postgres transform execution with error handling
4. **Robust Error Handling**: Graceful degradation if DBT setup fails
5. **Comprehensive Logging**: Clear step-by-step progress reporting

### Service Waiting Logic
The script now includes intelligent waiting for services:
- **Container status verification**: Checks if containers are running before executing commands
- **Service readiness checks**: Tests actual service responsiveness (not just container status)
- **Timeout handling**: 60-second timeout with descriptive error messages
- **Consistent connection strings**: Uses `kafka:9094` for internal container communication

### Execution Flow
```bash
# Run with default batch size (100 events)
bash scripts/run_demo.sh

# Run with custom batch size
bash scripts/run_demo.sh 500

# Run with environment variables
export BATCH_SIZE=200
export SEED=123
bash scripts/run_demo.sh
```

## License

MIT

## Open Data Product Standard (ODPS) Definitions

The EAM Data Pipe Demo now includes Open Data Product Standard (ODPS) v4.1 definitions for all datasets across the Bronze, Silver, Silver-S5000F, and Gold layers. These definitions transform technical datasets into business-ready data products with clear value propositions, ownership, and consumption semantics.

### Key Features of ODPS
- **Product Metadata**: Name, description, owner, pricing model, SLAs, usage terms
- **Product Variants**: Packaging for internal and external consumers
- **Product Lineage**: Source-to-product traceability
- **Realistic Pricing Models**: Free for internal use, subscription-based for external consumers
- **Compliance**: Usage terms and restrictions documented

### ODPS Layers
- **Bronze**: Raw CDC events from the simulated EAM system
- **Silver**: Current-state application data derived from CDC resolution
- **Silver-S5000F**: Standardized maintenance lifecycle data aligned with ASD S5000F concepts
- **Gold**: Analytics-ready outputs providing actionable insights

See the [ODPS Data Products Documentation](docs/data_products/README.md) for detailed product definitions.
