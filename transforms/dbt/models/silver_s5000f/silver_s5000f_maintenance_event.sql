-- models/silver_s5000f/silver_s5000f_maintenance_event.sql
-- Silver-S5000F layer: MaintenanceEvent (executed maintenance)

{{ config(
    materialized='table',
    schema='silver_s5000f',
    tags=['silver_s5000f', 's5000f', 'standards'],
    indexes=[
        {'columns': ['maintenance_event_id'], 'properties': {'unique': true}},
        {'columns': ['source_id']},
    ]
) }}

select
    maintenance_event_id,
    product_instance_id,
    maintenance_task_id,
    event_type,
    actual_start_date,
    actual_end_date,
    source_system,
    source_id,
    mapped_at,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_silver_s5000f', 'maintenance_event') }}
