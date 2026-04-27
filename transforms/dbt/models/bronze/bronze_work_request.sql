-- models/bronze/bronze_work_request.sql
{{ config(
    materialized='table',
    schema='bronze',
    tags=['bronze', 'cdc'],
    indexes=[{'columns': ['pk_id']}, {'columns': ['event_date']}]
) }}

select
    entity,
    op,
    event_ts,
    pk_id,
    payload_json,
    event_date,
    source_system,
    dbt_created_at as dbt_loaded_at
from {{ source('iceberg_bronze', 'work_request') }}
