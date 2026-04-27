-- models/gold/gold_maintenance_history.sql
-- Gold layer: Historical maintenance activity by asset

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
    work_order_count,
    completed_actions_count,
    last_maintenance_date,
    computed_at,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_gold', 'maintenance_history') }}
