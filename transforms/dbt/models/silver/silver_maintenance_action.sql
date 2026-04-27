-- models/silver/silver_maintenance_action.sql
{{ config(
    materialized='table',
    schema='silver',
    tags=['silver', 'application_state'],
    indexes=[
        {'columns': ['id'], 'properties': {'unique': true}},
        {'columns': ['source_id']},
    ]
) }}

select
    id,
    work_order_id,
    step_number,
    description,
    status,
    started_at,
    completed_at,
    source_system,
    source_id,
    last_op,
    last_event_ts,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_silver', 'maintenance_action') }}
