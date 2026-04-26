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
           │ Airflow
           ▼
┌──────────────────────────┐
│ Iceberg Silver           │
│ Application State       │
└──────────┬──────────────┘
           │ Polars
           ▼
┌──────────────────────────┐
│ Iceberg Silver‑S5000F    │
│ Standardised Structures │
└──────────┬──────────────┘
           │
           ▼
┌──────────────────────────┐
│ Iceberg Gold             │
│ Analytics / Reporting   │
└──────────────────────────┘


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
│       └── gold_rollups.py
│
├── transforms/
│   └── polars/
│       ├── app/
│       │   ├── merge_asset.py
│       │   └── merge_work_order.py
│       └── s5000f/
│           ├── product_instance.py
│           ├── maintenance_task.py
│           ├── maintenance_task_step.py
│           └── maintenance_event.py
│
├── lake/
│   ├── bronze/
│   ├── silver/
│   ├── silver_s5000f/
│   └── gold/
│
├── docs/
│   ├── architecture.md
│   └── s5000f_mapping.md
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

Phase 6 — Iceberg Query Function with Polars SQL
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

Phase 7 — Data Contracts in ODCS YAML
Purpose: Define explicit dataset contracts for all Bronze, Silver, Silver-S5000F, and Gold tables in Open Data Contract Standard YAML.
Implementation:
- Create ODCS YAML contracts for each dataset layer
- Document dataset name, description, field schema, types, partition keys, and lineage metadata
- Cover Bronze CDC datasets: cdc.asset, cdc.work_request, cdc.work_order, cdc.maintenance_action
- Cover Silver current-state tables: asset, work_request, work_order, maintenance_action
- Cover Silver-S5000F tables: product_instance, maintenance_task, maintenance_task_step, maintenance_event
- Cover Gold analytics outputs: asset_availability, maintenance_history, work_order_backlog, mtbf_metrics
Key Features:
✅ ODCS YAML contract artifacts for all layers
 ✅ Explicit schema definitions and metadata
 ✅ Layer-aware contract boundaries and lineage links
 ✅ Consumer expectations and partition semantics documented
Files to create:
- docs/data_contracts/bronze_odcs.yaml
- docs/data_contracts/silver_odcs.yaml
- docs/data_contracts/silver_s5000f_odcs.yaml
- docs/data_contracts/gold_odcs.yaml

11. Success Criteria
The PoC is successful if:
✅ CDC flows from simulator to Kafka
 ✅ Iceberg Bronze and Silver populate correctly
 ✅ S5000F‑aligned tables are generated
 ✅ Source‑to‑standard traceability is demonstrable
 ✅ Gold analytics execute successfully
 ✅ Iceberg query function provides unified access to all data layers

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
