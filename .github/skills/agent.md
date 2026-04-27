AGENT.md
1. Purpose
This repository is a minimal proof‑of‑concept (PoC) demonstrating a modern data‑engineering architecture for Engineering & Asset Management (EAM), extended to include transformation into ASD S5000F‑aligned data structures.
The PoC demonstrates how proprietary EAM application schemas can be:
captured via CDC,
analytically stabilised,
and mapped into an internationally recognised maintenance & logistics data standard (ASD S5000F).
This PoC is illustrative, not a complete S5000F implementation.

2. High‑Level Architecture
┌──────────────────────────┐
│ Simulated EAM System     │
│ (Application Schema)    │
└──────────┬──────────────┘
           │ CDC Events
           ▼
┌──────────────────────────┐
│ Apache Kafka             │
│ (CDC Topics)             │
└──────────┬──────────────┘
           │
           ▼
┌──────────────────────────┐
│ Iceberg Bronze           │
│ Raw CDC (App Schema)    │
└──────────┬──────────────┘
           │ Airflow + DBT
           ├─────────────────────────┐
           ▼                         ▼
┌──────────────────────────┐  ┌──────────────────────────┐
│ Iceberg Silver           │  │ Postgres Bronze/Silver   │
│ Application State       │  │ (Analytic Replica)      │
└──────────┬──────────────┘  └──────────────────────────┘
           │ Polars
           ├─────────────────────────┐
           ▼                         ▼
┌──────────────────────────┐  ┌──────────────────────────┐
│ Iceberg Silver‑S5000F    │  │ Postgres S5000F Tables   │
│ Standardised Structures │  │ (Analytic Replica)      │
└──────────┬──────────────┘  └──────────────────────────┘
           │
           ├─────────────────────────┐
           ▼                         ▼
┌──────────────────────────┐  ┌──────────────────────────┐
│ Iceberg Gold             │  │ Postgres Gold            │
│ Analytics / Reporting   │  │ (BI / Analytics Tables) │
└──────────────────────────┘  └──────────────────────────┘


3. Domain Scope
The PoC models Engineering & Asset Management in a simplified industrial plant environment and demonstrates semantic interoperability between:
EAM application entities (operational view)
ASD S5000F concepts (maintenance lifecycle view)

4. Simulated EAM Source System
4.1 Purpose
The simulated EAM system:
Maintains entity state in-memory
Emits CDC‑style events on create/update/delete
Exposes no analytical tables
This enforces a clean separation between operational systems and analytical / standardised models.

4.2 Application Entities
Entity
Description
Asset
Physical plant equipment
WorkRequest
Reported defect, issue, or need
WorkOrder
Approved and scheduled maintenance work
MaintenanceAction
Executed step within a maintenance task

4.3 CDC Event Model
All CDC events use a common envelope:
JSON
{
"entity": "Asset | WorkRequest | WorkOrder | MaintenanceAction",
"op": "c | u | d",
"event_ts": "ISO‑8601 timestamp",
"pk": { "id": "string" },
"before": { },
"after": { },
"source": {
"system": "simulated-eam",
"version": "poc-v1"
}
}
Show more lines

4.4 Kafka Topics
One topic per entity:
cdc.asset
cdc.work_request
cdc.work_order
cdc.maintenance_action

Topics are append‑only and partitioned by primary key.

5. Data Lake Design (Iceberg)
5.1 Bronze Layer — Raw CDC (Application Schema)
Exact CDC payloads
No semantic transformation
Append‑only
Partitioned by event_date

5.2 Silver Layer — Application State
CDC resolved into current‑state tables
Cleaned but unchanged application semantics
One row per entity instance
Purpose:
Stabilise and normalise operational data before semantic transformation.

5.3 Silver‑S5000F Layer — Standardised Maintenance Model
This layer demonstrates alignment with ASD S5000F concepts.
Generated via Polars batch jobs
Preserves traceability to source entities
Represents maintenance lifecycle concepts
Example tables:
silver_s5000f.product_instance
silver_s5000f.maintenance_task
silver_s5000f.maintenance_event
silver_s5000f.maintenance_task_step

This layer is conceptually compliant, not formally certified.

5.4 Gold Layer — Analytics & Reporting
Derived, analytics‑ready outputs including:
Asset availability
Work‑order backlog
Maintenance history by asset
Simplified MTBF‑style metrics
Gold tables may be derived from:
Application Silver
Silver‑S5000F
Or both (for comparison)

6. ASD S5000F Conceptual Alignment (PoC Scope)
6.1 S5000F Concepts Implemented
S5000F Concept
Description
Derived From
ProductInstance
Maintainable physical item
Asset
FunctionalFailure
Reported failure mode
WorkRequest
MaintenanceTask
Planned maintenance
WorkOrder
MaintenanceTaskStep
Atomic work activity
MaintenanceAction
MaintenanceEvent
Executed maintenance
WorkOrder + Actions

6.2 Identity & Traceability Rules
Deterministic IDs derived from source PKs
source_system and source_id preserved
Point‑in‑time correctness via Iceberg snapshots

6.3 Temporal Rules
planned_* timestamps → planning attributes
actual_* timestamps → execution attributes
Open or unexecuted work orders do not produce MaintenanceEvents

6.4 Explicit Scope Limitations
This PoC does not attempt:
Configuration breakdown structures
Applicability logic
Supply chain or spares modelling
XML / XSD interchange conformance
It demonstrates semantic interoperability, not certification.

7. Polars Transformation Responsibilities
Polars is the only batch transformation engine.
Phase A — CDC Resolution (Bronze → Silver)
Resolve application CDC
Partition‑replace semantics
One job per entity

Phase B — Semantic Standardisation (Silver → Silver‑S5000F)
Map application entities to S5000F concepts
Apply light business rules
Preserve lineage
Generate S5000F‑aligned tables

Rules
✅ Partition‑scoped reads only
 ✅ Bounded datasets per run
 ✅ Idempotent outputs
 ✅ No streaming or unbounded joins

8. Airflow Orchestration Model
8.1 DAG Categories
DAG
Purpose
eam_simulator
Generate CDC events
bronze_ingest
Kafka → Iceberg Bronze
cdc_merge_*
Bronze → Application Silver
s5000f_transform_*
Application Silver → Silver‑S5000F
gold_rollups
Silver / S5000F → Gold

8.2 DAG Ordering
eam_simulator
  → bronze_ingest
    → cdc_merge_application
      → s5000f_transform
        → gold_rollups
          → dbt_postgres_transform

Airflow is strictly used for orchestration, not computation.

9. Repository Structure
.
├── eam_simulator/
│   ├── entities/
│   ├── event_generator.py
│   └── produce_cdc.py
│
├── airflow/
│   └── dags/
│       ├── eam_simulator_dag.py
│       ├── cdc_merge_application.py
│       ├── s5000f_transform.py
│       ├── gold_rollups.py
│       └── dbt_postgres_transform_dag.py
│
├── transforms/
│   ├── polars/
│   │   ├── app/
│   │   │   ├── merge_asset.py
│   │   │   └── merge_work_order.py
│   │   ├── s5000f/
│   │   │   ├── product_instance.py
│   │   │   ├── maintenance_task.py
│   │   │   ├── maintenance_task_step.py
│   │   │   └── maintenance_event.py
│   │   └── query/
│   │       ├── iceberg_query.py
│   │       └── examples/
│   │           ├── asset_availability_query.py
│   │           ├── maintenance_history_query.py
│   │           ├── s5000f_compliance_query.py
│   │           └── cross_layer_comparison.py
│   └── dbt/
│       ├── models/
│       │   ├── bronze/
│       │   ├── silver/
│       │   ├── silver_s5000f/
│       │   ├── gold/
│       │   └── staging/
│       ├── seeds/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── README.md
│
├── lake/
│   ├── bronze/
│   ├── silver/
│   ├── silver_s5000f/
│   └── gold/
│
├── docs/
│   ├── architecture.md
│   ├── s5000f_mapping.md
│   ├── data_contracts/
│   │   ├── bronze_odcs.yaml
│   │   ├── silver_odcs.yaml
│   │   ├── silver_s5000f_odcs.yaml
│   │   └── gold_odcs.yaml
│   └── data_products/
│       ├── bronze_odps.yaml
│       ├── silver_odps.yaml
│       ├── silver_s5000f_odps.yaml
│       ├── gold_odps.yaml
│       └── README.md
│
└── README.md


10. Minimal Implementation Phases
Phase 1 — Local Infrastructure
Docker Compose
Kafka
Airflow
Object storage (MinIO or local FS)

Phase 2 — EAM Simulator
Python entity models
Random lifecycle transitions
CDC event production

Phase 3 — Application Bronze & Silver
Kafka ingestion
Iceberg Bronze tables
Polars CDC merge jobs

Phase 4 — S5000F Semantic Transformation
Polars mapping scripts
Silver‑S5000F Iceberg tables
Provenance and correctness checks

Phase 5 — Analytics & Demonstration
Gold metrics
Comparison of application vs S5000F analytics
End‑to‑end lineage demonstration

Phase 6 — DBT-Based Postgres Transform Layer
Purpose: Complement the Polars→Iceberg pipeline with a parallel DBT→Postgres layer for external BI tool integration and multi-sink analytical architecture. Demonstrate enterprise patterns for analytical replication and dimensional modeling.

Architectural Role:
- **Input**: Pre-computed Iceberg tables (Bronze, Silver, Silver-S5000F, Gold) from Polars jobs
- **Transform Engine**: dbt + Postgres SQL
- **Output**: Postgres tables organized by layer (bronze_*, silver_*, silver_s5000f_*, gold_*)
- **Use Case**: BI tools, external reporting, federated query scenarios
- **Data Flow**: Iceberg (source of truth) → DBT models → Postgres (analytical replica)

Core Principles:
✅ Polars remains the primary transformation engine (per copilot-instructions.md)
✅ DBT adds a secondary sink for external BI consumption
✅ Iceberg is the source of truth; Postgres is derived
✅ All four data layers modeled end-to-end
✅ Identity and lineage preserved through source columns

Implementation Architecture:
**DBT Project Structure** (`transforms/dbt/`)

```
transforms/dbt/
├── models/
│   ├── staging/
│   │   └── stg_iceberg_sources.sql    (Iceberg source definitions)
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
│   └── iceberg_catalog_mappings.csv   (Iceberg → Postgres table mappings)
├── dbt_project.yml
├── profiles.yml
└── README.md
```

**Two Architectural Options**:

**Option A: Secondary Sink (Recommended)**
- Iceberg is source of analytical truth
- Postgres is read-only analytical replica
- DBT seeds from Iceberg snapshots on schedule (daily/hourly)
- Lower freshness requirement (eventual consistency)
- Simpler data governance (Iceberg source controls)
- Use case: BI/dashboard serving, historical reporting
- Pros: Clear source-of-truth semantics, transactional consistency from Iceberg
- Cons: Data latency, dual infrastructure to maintain

**Option B: Primary Analytical Store (Alternative)**
- Postgres is the primary analytical database
- DBT includes dimensional modeling (facts/dimensions)
- Star schema for efficient BI querying
- Higher operational complexity (need CDC into Postgres)
- Pros: Native BI tool integration, query performance
- Cons: Dual responsibility (Iceberg for history, Postgres for analytics), sync complexity

Recommended Approach: **Option A** for PoC (simpler, clearer semantics)

Identity & Lineage Preservation:
- All Postgres tables include:
  - `source_system` — Origin system (simulated-eam)
  - `source_id` — Original entity ID from CDC
  - `iceberg_snapshot_id` — Iceberg snapshot version used for this load
  - `dbt_created_at` — DBT execution timestamp
  - `dbt_updated_at` — Latest DBT model update
- Foreign key relationships maintained across layers
- Traceability: Source ID → Iceberg entity → Postgres record

Integration with Airflow:
- New DAG: `dbt_postgres_transform_dag.py`
- Runs after `gold_rollups_dag.py` completes
- Tasks:
  1. `dbt_run_bronze` — Load Bronze Postgres tables
  2. `dbt_run_silver` — Load Silver Postgres tables
  3. `dbt_run_silver_s5000f` — Load Silver-S5000F Postgres tables
  4. `dbt_run_gold` — Load Gold Postgres tables
  5. `dbt_test` — Validate Postgres table schemas and PK constraints
- Parameterized by partition date (inherited from Iceberg snapshot)

Iceberg ↔ Postgres Connectivity:
- **Connection Method**: PyIceberg client (or dbt external query layer)
- **Staging Pattern**: Iceberg tables exposed as external views/stage in Postgres
- **Alternative**: Use Dremio/Starburst federation for transparent cross-database queries
- Documented as "implementation detail; out of scope for Phase 6 MVP"

Data Freshness & SLAs:
- Option A (Recommended): Daily snapshots from Iceberg → Postgres (24h freshness SLA)
- Option B (If used): Near-real-time via Kafka → Postgres CDC (minutes)

Key Deliverables:
✅ DBT project scaffold with all four layer models
✅ dbt_postgres_transform_dag.py orchestration
✅ Profile configuration for Postgres adapter
✅ Source definitions for Iceberg tables
✅ Schema tests (unique keys, not nulls)
✅ Documentation of Option A vs Option B trade-offs
✅ Example queries demonstrating cross-layer analytical joins in Postgres

Phase 7 — Iceberg Query Function with Polars SQL
Purpose: Provide a unified query interface for analytical exploration across all data layers
Implementation:
- Create a reusable Polars SQL query function
- Support querying Silver, Silver-S5000F, and Gold tables
- Enable cross-layer joins and comparisons
- Provide parameterized date partitioning
- Include example queries for common analytical patterns
Key Features:
✅ Single function to query any Iceberg table
 ✅ Support for Polars SQL syntax
 ✅ Automatic partition filtering by date
 ✅ Cross-layer join capabilities
 ✅ Example queries for demonstration
Files to create:
- transforms/polars/query/iceberg_query.py
- transforms/polars/query/examples/
  - asset_availability_query.py
  - maintenance_history_query.py
  - s5000f_compliance_query.py
  - cross_layer_comparison.py

Phase 8 — Open Data Product Standard (ODPS) v4.1 Implementation
Purpose: Define realistic and representative data products for all Bronze, Silver, Silver-S5000F, and Gold layers using Open Data Product Standard v4.1. Transform technical datasets into marketable data products with business value, clear ownership, pricing, and consumption semantics.

Implementation:
- Create ODPS v4.1 YAML product definitions for each data product layer
- Define data products as marketable assets with business context, not just technical contracts
- Include product metadata: name, description, owner, pricing model, SLAs, and usage terms
- Map each Iceberg table to a data product with clear value proposition
- Define product variants for different consumer segments (internal vs external)
- Implement product versioning and lifecycle management
- Cover Bronze data products: Raw CDC Streams (Asset, WorkRequest, WorkOrder, MaintenanceAction)
- Cover Silver data products: Current-State Entity Views (Asset, WorkRequest, WorkOrder, MaintenanceAction)
- Cover Silver-S5000F data products: Standardised Maintenance Products (ProductInstance, MaintenanceTask, MaintenanceEvent, MaintenanceTaskStep)
- Cover Gold data products: Analytical Insights (AssetAvailability, MaintenanceHistory, WorkOrderBacklog, MTBFMetrics)

Key Features:
✅ ODPS v4.1 compliant product definitions for all layers
 ✅ Business-oriented product metadata (value proposition, pricing, SLAs)
 ✅ Clear product ownership and stewardship
 ✅ Product variants for different consumer segments
 ✅ Integration with existing ODCS technical contracts
 ✅ Product lifecycle and version management
 ✅ Realistic pricing models (free for internal, subscription for external)
 ✅ Usage terms and compliance requirements documented

ODPS v4.1 Product Structure:
- Product Identity: Unique identifier, name, version
- Product Description: Business value, use cases, target audience
- Product Composition: Technical components (Iceberg tables, schemas)
- Product Variants: Different packaging for different consumers
- Product Pricing: Pricing model, tiers, billing frequency
- Product SLAs: Availability, freshness, support commitments
- Product Terms: Usage rights, restrictions, compliance requirements
- Product Lineage: Source-to-product traceability

Files to create:
- docs/data_products/bronze_odps.yaml
- docs/data_products/silver_odps.yaml
- docs/data_products/silver_s5000f_odps.yaml
- docs/data_products/gold_odps.yaml
- docs/data_products/README.md (ODPS implementation guide)

11. Success Criteria
The PoC is successful if:
✅ CDC flows from simulator to Kafka
 ✅ Iceberg Bronze and Silver populate correctly
 ✅ S5000F‑aligned tables are generated
 ✅ Source‑to‑standard traceability is demonstrable
 ✅ Gold analytics execute successfully
 ✅ Iceberg query function provides unified access to all data layers
 ✅ DBT Postgres layer populates with all four data layers (Bronze, Silver, Silver-S5000F, Gold)
 ✅ Postgres tables preserve source lineage and identity from Iceberg
 ✅ ODPS v4.1 data products are defined for all layers with realistic business metadata

12. Guiding Principle
Demonstrate semantic interoperability with the smallest working system.
Prefer:
Explicit mapping logic
Clear lineage
Bounded computation
Readable Polars code
Avoid:
Over‑engineering S5000F
Hidden abstractions
Full standard completeness
