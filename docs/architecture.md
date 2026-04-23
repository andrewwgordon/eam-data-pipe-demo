# Architecture Overview

## Purpose

This PoC demonstrates a modern data-engineering architecture for Engineering & Asset Management (EAM), extended to include transformation into ASD S5000F-aligned data structures.

## High-Level Data Flow

```
┌──────────────────────────┐
│ Simulated EAM System     │
│ (Python in-memory state) │
└──────────┬───────────────┘
           │ CDC Events (JSON)
           ▼
┌──────────────────────────┐
│ Apache Kafka             │
│ 4 topics (one per entity)│
└──────────┬───────────────┘
           │ Consume
           ▼
┌──────────────────────────┐
│ Iceberg Bronze           │
│ Raw CDC (append-only)    │
└──────────┬───────────────┘
           │ Polars CDC merge
           ▼
┌──────────────────────────┐
│ Iceberg Silver           │
│ Application current state│
└──────────┬───────────────┘
           │ Polars semantic mapping
           ▼
┌──────────────────────────┐
│ Iceberg Silver-S5000F    │
│ Standardised structures  │
└──────────┬───────────────┘
           │ Polars aggregation
           ▼
┌──────────────────────────┐
│ Iceberg Gold             │
│ Analytics & reporting    │
└──────────────────────────┘
```

## Infrastructure Components

| Component | Image | Purpose |
|-----------|-------|---------|
| Kafka | `bitnami/kafka:3.7` (KRaft) | CDC event transport |
| MinIO | `minio/minio:latest` | S3-compatible object storage |
| Iceberg REST Catalog | `tabulario/iceberg-rest:1.6.0` | Table metadata management |
| Airflow | `apache/airflow:2.9.3-python3.11` | Pipeline orchestration |

## Kafka Topics

| Topic | Entity | Partitions |
|-------|--------|------------|
| `cdc.asset` | Asset | 4 |
| `cdc.work_request` | WorkRequest | 4 |
| `cdc.work_order` | WorkOrder | 4 |
| `cdc.maintenance_action` | MaintenanceAction | 4 |

All topics are append-only, partitioned by primary key.

## Data Lake Layers

### Bronze — Raw CDC
- Exact CDC payloads stored as JSON strings
- Append-only, no deduplication
- Partitioned by `event_date`

### Silver — Application State
- CDC resolved into current-state tables (one row per entity)
- Application semantics preserved
- Written via partition-replace (idempotent)

### Silver-S5000F — Standardised Model
- Semantic transformation into ASD S5000F concepts
- Deterministic IDs derived from source PKs
- Full traceability to source entities

### Gold — Analytics
- Derived metrics: availability, backlog, history, MTBF
- Can use Silver or Silver-S5000F as inputs

## Airflow DAG Pipeline

```
eam_simulator
  → bronze_ingest
    → cdc_merge_application (4 parallel tasks)
      → s5000f_transform (5 tasks with dependencies)
        → gold_rollups (4 parallel tasks)
```

## Technology Constraints

- **Polars only** — no Spark, DuckDB, or Pandas
- **Kafka is transport** — append-only, no replay logic
- **Airflow orchestrates** — no business logic in DAGs
- **Bounded computation** — no unbounded table scans
