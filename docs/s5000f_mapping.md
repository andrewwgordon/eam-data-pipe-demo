# ASD S5000F Mapping Documentation

## Overview

This document describes the semantic mapping from application EAM entities to ASD S5000F-aligned concepts. The mapping is **conceptual** — it demonstrates semantic interoperability, not formal S5000F compliance.

## Concept Mapping Summary

| S5000F Concept | Source Entity | Description |
|----------------|---------------|-------------|
| ProductInstance | Asset | Maintainable physical item |
| FunctionalFailure | WorkRequest | Reported failure mode |
| MaintenanceTask | WorkOrder | Planned maintenance activity |
| MaintenanceTaskStep | MaintenanceAction | Atomic work step |
| MaintenanceEvent | WorkOrder + Actions | Executed maintenance |

## Detailed Field Mappings

### ProductInstance ← Asset

| S5000F Field | Source Field | Notes |
|-------------|-------------|-------|
| `product_instance_id` | `asset.id` | Deterministic hash: `PI-{sha256(ProductInstance:id)[:12]}` |
| `name` | `asset.name` | Direct mapping |
| `type` | `asset.asset_type` | Direct mapping |
| `location` | `asset.location` | Direct mapping |
| `operational_status` | `asset.status` | Direct mapping (operational/degraded/failed) |
| `installation_date` | `asset.install_date` | Direct mapping |
| `source_system` | — | Always `simulated-eam` |
| `source_id` | `asset.id` | Original PK for traceability |

### FunctionalFailure ← WorkRequest

| S5000F Field | Source Field | Notes |
|-------------|-------------|-------|
| `functional_failure_id` | `work_request.id` | Deterministic hash: `FF-{hash}` |
| `product_instance_id` | `work_request.asset_id` | Hash of the linked asset ID |
| `failure_description` | `work_request.description` | Direct mapping |
| `priority` | `work_request.priority` | Direct mapping |
| `reported_at` | `work_request.reported_at` | Direct mapping |

### MaintenanceTask ← WorkOrder

| S5000F Field | Source Field | Notes |
|-------------|-------------|-------|
| `maintenance_task_id` | `work_order.id` | Deterministic hash: `MT-{hash}` |
| `product_instance_id` | `work_order.asset_id` | Hash of the linked asset ID |
| `functional_failure_id` | `work_order.work_request_id` | Hash of the linked work request ID |
| `status` | `work_order.status` | Direct mapping |
| `planned_start` | `work_order.planned_start` | Planning attribute |
| `planned_end` | `work_order.planned_end` | Planning attribute |

### MaintenanceTaskStep ← MaintenanceAction

| S5000F Field | Source Field | Notes |
|-------------|-------------|-------|
| `task_step_id` | `maintenance_action.id` | Deterministic hash: `MTS-{hash}` |
| `maintenance_task_id` | `maintenance_action.work_order_id` | Hash of the linked work order ID |
| `step_number` | `maintenance_action.step_number` | Direct mapping |
| `description` | `maintenance_action.description` | Direct mapping |
| `status` | `maintenance_action.status` | Direct mapping |
| `started_at` | `maintenance_action.started_at` | Execution attribute |
| `completed_at` | `maintenance_action.completed_at` | Execution attribute |

### MaintenanceEvent ← WorkOrder + MaintenanceAction

| S5000F Field | Source Field | Notes |
|-------------|-------------|-------|
| `maintenance_event_id` | `work_order.id` | Deterministic hash: `ME-{hash}` |
| `maintenance_task_id` | `work_order.id` | Hash linking to MaintenanceTask |
| `product_instance_id` | `work_order.asset_id` | Hash of the linked asset ID |
| `actual_start` | `work_order.actual_start` | Execution attribute |
| `actual_end` | `work_order.actual_end` | Execution attribute |
| `action_count` | COUNT(actions) | Number of completed actions |

**Temporal Rule:** Only work orders with `status = 'completed'` AND `actual_start IS NOT NULL` produce MaintenanceEvents.

## Identity & Traceability

- All S5000F IDs are **deterministic**: `{prefix}-{sha256(concept:source_id)[:12]}`
- Every S5000F record preserves `source_system` and `source_id`
- Point-in-time correctness via Iceberg snapshots

## Scope Limitations

This PoC does **not** implement:
- Configuration breakdown structures
- Applicability logic
- Supply chain or spares modelling
- XML/XSD interchange conformance
- Certification-level completeness

The goal is to demonstrate **semantic interoperability** with the smallest working system.
