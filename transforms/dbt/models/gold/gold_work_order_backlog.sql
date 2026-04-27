-- models/gold/gold_work_order_backlog.sql
-- Gold layer: Current open work orders and queue depth

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
    work_order_id,
    priority,
    status,
    days_open,
    computed_at,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_gold', 'work_order_backlog') }}
