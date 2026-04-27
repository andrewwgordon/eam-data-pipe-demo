-- models/gold/gold_asset_availability.sql
-- Gold layer: Asset operational availability analytics

{{ config(
    materialized='table',
    schema='gold',
    tags=['gold', 'analytics'],
    indexes=[
        {'columns': ['asset_id']},
    ]
) }}

select
    asset_id,
    name,
    asset_type,
    location,
    status,
    is_available,
    availability_pct,
    computed_at,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_gold', 'asset_availability') }}
