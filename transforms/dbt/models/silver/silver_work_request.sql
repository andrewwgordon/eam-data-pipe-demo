-- models/silver/silver_work_request.sql
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
    asset_id,
    description,
    priority,
    status,
    reported_at,
    updated_at,
    source_system,
    source_id,
    last_op,
    last_event_ts,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_silver', 'work_request') }}
