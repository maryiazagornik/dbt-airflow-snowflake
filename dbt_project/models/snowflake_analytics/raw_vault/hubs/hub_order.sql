{{ config(
    materialized='incremental',
    unique_key=['ORDER_PK', 'LOAD_DATE'],
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        SHA2(COALESCE(TO_VARCHAR(ORDER_ID), ''), 256) AS ORDER_PK,
        ORDER_ID,
        LOAD_DATE,
        RECORD_SOURCE
    FROM {{ ref('stg_orders') }}
)

SELECT * FROM source_data
{% if is_incremental() %}
    WHERE LOAD_DATE > (
        SELECT COALESCE(MAX(LOAD_DATE), '1900-01-01'::date) 
        FROM {{ this }}
    )
{% endif %}
