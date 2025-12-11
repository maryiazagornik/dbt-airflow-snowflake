{{ config(
    materialized='incremental',
    unique_key='ORDER_PK'
) }}

WITH source_data AS (
    SELECT
        ORDER_PK,
        ORDER_DATE,
        ORDER_PRIORITY,
        O_CLERK,
        TOTAL_PRICE,
        LOAD_DATE,
        RECORD_SOURCE,
        HASHDIFF_DETAILS
    FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE LOAD_DATE > (SELECT MAX(t.LOAD_DATE) FROM {{ this }} AS t)
    {% endif %}
)

SELECT * FROM source_data AS s
{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS t
        WHERE t.ORDER_PK = s.ORDER_PK
    )
{% endif %}
