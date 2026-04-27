-- models/silver_s5000f/silver_s5000f_maintenance_task_step.sql
-- Silver-S5000F layer: MaintenanceTaskStep (atomic work activity)

{{ config(
    materialized='table',
    schema='silver_s5000f',
    tags=['silver_s5000f', 's5000f', 'standards'],
    indexes=[
        {'columns': ['maintenance_task_step_id'], 'properties': {'unique': true}},
        {'columns': ['source_id']},
    ]
) }}

select
    maintenance_task_step_id,
    maintenance_task_id,
    step_sequence,
    description,
    status,
    source_system,
    source_id,
    mapped_at,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_silver_s5000f', 'maintenance_task_step') }}
