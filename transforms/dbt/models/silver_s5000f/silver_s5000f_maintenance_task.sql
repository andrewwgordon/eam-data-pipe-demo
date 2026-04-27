-- models/silver_s5000f/silver_s5000f_maintenance_task.sql
-- Silver-S5000F layer: MaintenanceTask (planned maintenance)

{{ config(
    materialized='table',
    schema='silver_s5000f',
    tags=['silver_s5000f', 's5000f', 'standards'],
    indexes=[
        {'columns': ['maintenance_task_id'], 'properties': {'unique': true}},
        {'columns': ['source_id']},
    ]
) }}

select
    maintenance_task_id,
    product_instance_id,
    task_type,
    status,
    planned_start_date,
    planned_end_date,
    source_system,
    source_id,
    mapped_at,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_silver_s5000f', 'maintenance_task') }}
