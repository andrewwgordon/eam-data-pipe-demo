# DBT Postgres Transform Layer (Phase 6)

This directory contains the DBT project for replicating and transforming EAM data from Iceberg into Postgres for BI tool integration.

## Architecture

**Data Flow**: Iceberg (source of truth) → Postgres staging schemas → DBT models → Postgres analytics schemas

### Layers

- **Bronze**: Raw CDC events from Iceberg Bronze, replicated 1:1
- **Silver**: Application state tables from Iceberg Silver, deduplicated and lineage-preserved
- **Silver-S5000F**: Standards-aligned tables from Iceberg Silver-S5000F
- **Gold**: Analytics-ready tables from Iceberg Gold

## Setup

### Prerequisites

1. Postgres database running (e.g., via Docker)
2. DBT and dbt-postgres installed: `pip install dbt-core dbt-postgres`
3. Iceberg tables populated (Bronze, Silver, Silver-S5000F, Gold)
4. Airflow with dbt_postgres_transform_dag running

### Environment Configuration

Set these environment variables or create a `.env` file:

```bash
# Postgres connection
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres
export POSTGRES_DB=eam_analytics

# Iceberg catalog
export ICEBERG_CATALOG_URI=http://localhost:8181

# MinIO (if needed)
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
```

### DBT Profiles

Copy `profiles.yml` to `~/.dbt/profiles.yml` or use `DBT_PROFILES_DIR` environment variable:

```bash
export DBT_PROFILES_DIR=$(pwd)
```

### Database Setup

Create the Postgres database and schemas:

```bash
psql -U postgres -c "CREATE DATABASE eam_analytics;"
psql -U postgres -d eam_analytics -c "CREATE SCHEMA IF NOT EXISTS staging_bronze;"
psql -U postgres -d eam_analytics -c "CREATE SCHEMA IF NOT EXISTS staging_silver;"
psql -U postgres -d eam_analytics -c "CREATE SCHEMA IF NOT EXISTS staging_silver_s5000f;"
psql -U postgres -d eam_analytics -c "CREATE SCHEMA IF NOT EXISTS staging_gold;"
psql -U postgres -d eam_analytics -c "CREATE SCHEMA IF NOT EXISTS bronze;"
psql -U postgres -d eam_analytics -c "CREATE SCHEMA IF NOT EXISTS silver;"
psql -U postgres -d eam_analytics -c "CREATE SCHEMA IF NOT EXISTS silver_s5000f;"
psql -U postgres -d eam_analytics -c "CREATE SCHEMA IF NOT EXISTS gold;"
```

## Running DBT

### Development (Local)

```bash
# Test connection
dbt debug

# Run all models
dbt run

# Run models by tag
dbt run --selector tag:bronze
dbt run --selector tag:silver
dbt run --selector tag:silver_s5000f
dbt run --selector tag:gold

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

### Production (via Airflow)

The DAG `dbt_postgres_transform_dag` orchestrates:

1. **Extract** Iceberg tables to Postgres staging schemas (Python operators)
2. **Transform** via DBT models (BashOperators running `dbt run --selector`)
3. **Validate** via DBT tests (BashOperator running `dbt test`)

Schedule: Daily after `gold_rollups_dag` completes

## Project Structure

```
transforms/dbt/
├── models/
│   ├── staging/
│   │   ├── sources.yml (source definitions)
│   │   └── src_iceberg_bronze.sql
│   ├── bronze/
│   │   ├── bronze_asset.sql
│   │   ├── bronze_work_request.sql
│   │   ├── bronze_work_order.sql
│   │   └── bronze_maintenance_action.sql
│   ├── silver/
│   │   ├── silver_asset.sql
│   │   ├── silver_work_request.sql
│   │   ├── silver_work_order.sql
│   │   └── silver_maintenance_action.sql
│   ├── silver_s5000f/
│   │   ├── silver_s5000f_product_instance.sql
│   │   ├── silver_s5000f_maintenance_task.sql
│   │   ├── silver_s5000f_maintenance_event.sql
│   │   └── silver_s5000f_maintenance_task_step.sql
│   └── gold/
│       ├── gold_asset_availability.sql
│       ├── gold_maintenance_history.sql
│       ├── gold_work_order_backlog.sql
│       └── gold_mtbf_metrics.sql
├── seeds/
├── macros/
├── tests/
├── dbt_project.yml (project configuration)
├── profiles.yml (Postgres target config)
└── README.md (this file)
```

## Key Design Decisions

### 1. Iceberg as Source of Truth

- DBT reads from Iceberg snapshots (loaded into Postgres staging schemas by Airflow)
- Postgres is a derived analytical replica, not operational storage
- Simplifies data governance and enables point-in-time recovery

### 2. Staging Pattern

- **Staging schemas** (`staging_*`): Load zone for Iceberg extracts
- **Analytics schemas** (`bronze`, `silver`, `silver_s5000f`, `gold`): Final DBT outputs
- Enables idempotent, repeatable transforms

### 3. Lineage Preservation

- All models retain `source_system`, `source_id`, `dbt_created_at`, `dbt_updated_at`
- Enables traceability: App entity → Iceberg → Postgres
- Foreign keys maintained across layer joins

### 4. Materialization Strategy

- **Views** (staging layer): Lightweight, ephemeral
- **Tables** (bronze, silver, s5000f, gold): Materialized for performance and BI tool consumption

## Integration with Airflow

The Airflow DAG `dbt_postgres_transform_dag` handles:

1. **Data Extraction** (Python tasks)
   - Reads Iceberg tables using PyIceberg
   - Loads into Postgres staging schemas
   - Adds metadata columns (dbt_created_at, dbt_updated_at)

2. **Data Transformation** (Bash tasks running dbt)
   - Transforms staging → analytics via DBT models
   - Serial execution: bronze → silver → s5000f → gold

3. **Data Validation** (Bash task running dbt tests)
   - Primary key uniqueness
   - Not-null constraints
   - Referential integrity

## Known Limitations & Future Enhancements

1. **Iceberg Connectivity**: Current implementation uses PyIceberg in Airflow to extract. Production use case may require:
   - Dremio/Starburst federation layer for transparent cross-database queries
   - External tables in Postgres pointing to S3/Iceberg
   - DBT external data packages

2. **Data Freshness**: Daily batch (24h SLA). For near-real-time:
   - Implement Kafka → Postgres staging (CDC)
   - Use DBT incremental models with CDC merge logic

3. **Dimensional Modeling**: Option B (Postgres as primary store) would require:
   - Fact/dimension star schema
   - Slowly changing dimensions
   - Conformed dimensions across layers

## Testing

Run DBT tests:

```bash
dbt test

# Test specific models
dbt test --select silver_asset

# Test specific tag
dbt test --selector tag:silver
```

Test files are defined in `models/*/schema.yml` files.

## Monitoring & Troubleshooting

### Common Issues

**Connection refused**:
```bash
dbt debug  # Verify Postgres connection
```

**Source table not found**:
```bash
# Verify staging schema has been populated by Airflow
psql -U postgres -d eam_analytics -c "\dt staging_*.*"
```

**DBT model failure**:
```bash
dbt run --debug  # Verbose logging
dbt run-operation debug  # Custom debugging
```

### Logs

- DBT logs: `target/dbt.log`
- Airflow logs: `/airflow/logs/dbt_postgres_transform_dag/`

## Contributing

- Add models in appropriate layer directory
- Update `sources.yml` with new source definitions
- Add schema tests for new tables
- Update this README for design changes

## References

- [DBT Documentation](https://docs.getdbt.com)
- [dbt-postgres Adapter](https://docs.getdbt.com/reference/warehouse-setups/postgres-setup)
- [Iceberg Concepts](https://iceberg.apache.org/concepts/)
- [S5000F Standard](https://www.s5000f.org/)
