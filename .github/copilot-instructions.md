GitHub Copilot Instructions
Engineering & Asset Management PoC (Polars + Iceberg + Kafka + Airflow + ASD S5000F)

1. Project Mission
This repository is a minimal proof‑of‑concept (PoC) demonstrating:
Modern data engineering patterns
Change Data Capture (CDC)
Canonical data modelling
Semantic transformation into ASD S5000F‑aligned data structures
The project focuses on Engineering & Asset Management (EAM) data and demonstrates how application‑specific schemas can be transformed into standardised, interoperable maintenance data.
This is a demonstrator, not a production system.

2. Architectural Principles (Non‑Negotiable)
When generating or modifying code, always respect the following principles:
Iceberg is the source of analytical truth

Treat Iceberg tables as immutable snapshots
Prefer partition‑replace patterns for updates
Polars is the only batch transformation engine

No Spark
No DuckDB
No Pandas for core transforms
Kafka is event transport, not a database

Append‑only
No replay logic in application code
Airflow only orchestrates

Airflow must not contain business logic
No heavy data processing in DAGs
Everything must be partition‑aware and bounded

No unbounded table scans
Always scope reads by date / batch

3. Data Layer Responsibilities
Bronze Layer
Raw CDC events
Application schema
No transformation except minimal normalisation
Silver Layer (Application)
CDC resolved into current state
Still application semantics
One record per entity instance
Silver‑S5000F Layer (Standardised)
Semantic transformation into ASD S5000F concepts
Explicit mapping code
Full traceability back to application entities
Gold Layer
Analytics and reporting
Use Silver or Silver‑S5000F as inputs

4. ASD S5000F Guidance (PoC Scope)
When generating S5000F‑related code, remember:
✅ Focus on conceptual alignment, not formal compliance
 ✅ Model lifecycle and maintenance semantics
 ✅ Keep mappings explicit and readable
❌ Do not implement XML, XSD, or full data exchange packages
 ❌ Do not attempt certification‑level completeness
Core S5000F Concepts Used
Concept
Meaning
ProductInstance
Maintainable physical item
FunctionalFailure
Reported failure or defect
MaintenanceTask
Planned maintenance activity
MaintenanceTaskStep
Atomic step
MaintenanceEvent
Executed maintenance

5. Coding Style Rules
Python
Use type hints
Prefer pure functions
Avoid hidden side effects
Keep scripts short and single‑purpose
Polars
Use lazy execution where appropriate (pl.LazyFrame)
Explicit column selection (no select("*"))
Keep transformations readable and staged
Never load more data than required
No‑Nos
No global mutable state
No silent exception handling
No hard‑coded paths (use parameters)

6. CDC Handling Rules
When dealing with CDC data:
Always include:

op (c/u/d)
event_ts
source_system
source_id
CDC merges must be:

Idempotent
Deterministic
Safe to rerun
Prefer partition replacement over row‑level mutation


7. Airflow DAG Expectations
When generating Airflow DAGs:
✅ Use clear, descriptive DAG IDs
 ✅ Keep DAGs small and composable
 ✅ Parameterise by date / batch
 ✅ Use one task per logical step
❌ Do not embed SQL or Polars logic in DAG files
 ❌ Do not create long‑running tasks

8. Repository Structure Awareness
Copilot must respect the existing repository structure.
eam_simulator/ → operational simulation only
transforms/polars/app/ → application CDC resolution
transforms/polars/s5000f/ → semantic standardisation
airflow/dags/ → orchestration only
docs/ → architecture & mapping documentation
Never mix concerns across directories.

9. Documentation Requirements
When creating or updating functionality:
Add inline comments explaining why, not just what
Update or create markdown docs if semantics change
Keep documentation concise and implementation‑focused
Prefer diagrams and mapping tables where helpful.

10. What Copilot Should Optimise For
✅ Clarity over cleverness
 ✅ Explicit transformations over abstractions
 ✅ Deterministic behaviour
 ✅ Reproducibility
 ✅ Educational value
This PoC is meant to be read, understood, and reasoned about by engineers and architects.

11. What Copilot Should Avoid
❌ Premature optimisation
 ❌ Over‑engineering
 ❌ Hidden magic
 ❌ Large frameworks
 ❌ Enterprise boilerplate

12. Mental Model to Use
“Build the smallest thing that clearly demonstrates
 CDC → Analytics → Semantic Standardisation → Insight.”
If a change does not serve that goal, do not implement it.
