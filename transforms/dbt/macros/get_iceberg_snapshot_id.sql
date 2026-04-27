# macros/get_iceberg_snapshot_id.sql
-- Macro to retrieve the Iceberg snapshot ID for lineage tracking

{% macro get_iceberg_snapshot_id(schema, table) %}
    {% set query %}
        SELECT MAX(iceberg_snapshot_id) as snapshot_id
        FROM {{ source('iceberg_' ~ schema, table) }}
    {% endset %}
    {% set result = run_query(query) %}
    {% if result %}
        {% set snapshot_id = result.rows[0].values()[0] %}
        {{ snapshot_id }}
    {% else %}
        'unknown'
    {% endif %}
{% endmacro %}
