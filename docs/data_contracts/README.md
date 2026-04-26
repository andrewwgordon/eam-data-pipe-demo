# Open Data Contract Standard (ODCS) Data Contracts

This directory contains the Open Data Contract Standard (ODCS) YAML definitions for the datasets in the EAM Data Pipe Demo. These contracts define the schema, metadata, and lineage for datasets across the Bronze, Silver, Silver-S5000F, and Gold layers.

## Purpose
The ODCS data contracts provide:
- Explicit schema definitions for each dataset
- Metadata including dataset description, owner, and partitioning
- Lineage information to trace data flow between layers
- A foundation for data governance and interoperability

## Data Contract Layers
The ODCS contracts are organized into the following layers:

### Bronze Layer
- Raw CDC events from the simulated EAM system
- Append-only datasets
- Partitioned by event date

### Silver Layer
- Current-state application data
- Derived from Bronze CDC resolution
- Cleaned and normalized for operational reporting

### Silver-S5000F Layer
- Standardized maintenance lifecycle data
- Aligned with ASD S5000F concepts
- Enables semantic interoperability

### Gold Layer
- Analytics-ready outputs
- Derived from Silver and Silver-S5000F layers
- Provides actionable insights for decision-making

## Directory Structure
```
.
├── bronze_odcs.yaml
├── silver_odcs.yaml
├── silver_s5000f_odcs.yaml
├── gold_odcs.yaml
└── README.md
```

## How to Use
1. **Explore Data Contracts**: Review the YAML files for each layer to understand the dataset schemas and metadata.
2. **Integrate with Consumers**: Use the lineage and schema definitions to integrate datasets into your workflows.
3. **Ensure Compliance**: Verify that datasets adhere to the defined schemas and metadata.
4. **Maintain Governance**: Update data contracts as datasets evolve to ensure consistency and traceability.

## Contact
For questions or support, contact the EAM Data Pipe Demo team at `eam-support@example.com`. 