-- models/silver/silver_asset.sql
-- Silver layer: Current-state Asset records
-- Replicated from Iceberg Silver.Asset

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
    name,
    asset_type,
    location,
    status,
    install_date,
    updated_at,
    source_system,
    source_id,
    last_op,
    last_event_ts,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_silver', 'asset') }}
