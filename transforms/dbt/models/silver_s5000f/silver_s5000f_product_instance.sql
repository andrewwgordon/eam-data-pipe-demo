-- models/silver_s5000f/silver_s5000f_product_instance.sql
-- Silver-S5000F layer: S5000F ProductInstance (maintainable physical items)

{{ config(
    materialized='table',
    schema='silver_s5000f',
    tags=['silver_s5000f', 's5000f', 'standards'],
    indexes=[
        {'columns': ['product_instance_id'], 'properties': {'unique': true}},
        {'columns': ['source_id']},
    ]
) }}

select
    product_instance_id,
    name,
    type,
    location,
    operational_status,
    installation_date,
    source_system,
    source_id,
    mapped_at,
    dbt_created_at,
    dbt_updated_at
from {{ source('iceberg_silver_s5000f', 'product_instance') }}
