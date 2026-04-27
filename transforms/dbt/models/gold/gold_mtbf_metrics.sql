-- models/gold/gold_mtbf_metrics.sql
-- Gold layer: Mean time between failures calculations

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
    asset_name,
    total_failures,
    observation_days,
    mtbf_days,
    computed_at,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_gold', 'mtbf_metrics') }}
