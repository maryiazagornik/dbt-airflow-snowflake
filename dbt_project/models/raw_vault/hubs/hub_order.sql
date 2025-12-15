{{ config(
    materialized='incremental',
    unique_key='ORDER_PK',
    incremental_strategy='merge'
) }}

SELECT DISTINCT
    ORDER_PK,
    ORDER_ID,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
    WHERE LOAD_DATE > (SELECT MAX(LOAD_DATE) FROM {{ this }})
{% endif %}
