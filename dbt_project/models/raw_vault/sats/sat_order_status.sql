{{ config(
    materialized='incremental',
    unique_key=['ORDER_PK', 'LOAD_DATE'],
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        SHA2(COALESCE(TO_VARCHAR(ORDER_ID), ''), 256) AS ORDER_PK,

        ORDER_STATUS,
        TOTAL_PRICE,
        LOAD_DATE,
        RECORD_SOURCE,

        SHA2(CONCAT(
            COALESCE(TO_VARCHAR(ORDER_STATUS), ''), '|',
            COALESCE(TO_VARCHAR(TOTAL_PRICE), '')
        ), 256) AS HASHDIFF_STATUS

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
          AND t.HASHDIFF_STATUS = s.HASHDIFF_STATUS
    )
{% endif %}
