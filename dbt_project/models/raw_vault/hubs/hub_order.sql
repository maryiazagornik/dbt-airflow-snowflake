{{ config(
    materialized='incremental',
    unique_key='ORDER_PK'
) }}

SELECT DISTINCT
    ORDER_PK,
    ORDER_ID,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
WHERE ORDER_PK NOT IN (SELECT ORDER_PK FROM {{ this }})
{% endif %}