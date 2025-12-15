{{ config(
    materialized='incremental',
    unique_key=['ORDER_PK', 'LOAD_DATE'],
    incremental_strategy='merge' 
) }}

WITH source_data AS (
    SELECT
        SHA2(COALESCE(TO_VARCHAR(ORDER_ID), ''), 256) AS ORDER_PK,
        
        ORDER_DATE,
        ORDER_PRIORITY,
        O_CLERK,
        TOTAL_PRICE,
        LOAD_DATE,
        RECORD_SOURCE,

        SHA2(CONCAT(
            COALESCE(TO_VARCHAR(ORDER_DATE), ''), '|',
            COALESCE(TO_VARCHAR(ORDER_PRIORITY), ''), '|',
            COALESCE(TO_VARCHAR(O_CLERK), '')
        ), 256) AS HASHDIFF_DETAILS

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
          AND t.HASHDIFF_DETAILS = s.HASHDIFF_DETAILS
    )
{% endif %}
