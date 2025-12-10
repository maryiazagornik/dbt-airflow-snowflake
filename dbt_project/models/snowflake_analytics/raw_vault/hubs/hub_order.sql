{{ config(
    materialized='incremental',
    unique_key='ORDER_PK'
) }}

SELECT DISTINCT
    source.ORDER_PK,
    source.ORDER_ID,
    source.LOAD_DATE,
    source.RECORD_SOURCE
FROM {{ ref('stg_orders') }} AS source

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{ this }} AS target
        WHERE target.ORDER_PK = source.ORDER_PK
    )
{% endif %}
