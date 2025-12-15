{{ config(materialized='incremental', unique_key='ORDER_PK') }}

SELECT DISTINCT
    ORDER_PK,
    
    ORDER_DATE,
    TOTAL_PRICE,
    ORDER_PRIORITY,
    O_CLERK as CLERK_NAME,
    SHIP_PRIORITY,
    ORDER_COMMENT,
    
    LOAD_DATE,
    RECORD_SOURCE,
    {{ dbt_utils.generate_surrogate_key(['ORDER_DATE', 'TOTAL_PRICE', 'ORDER_PRIORITY']) }} as HASHDIFF

FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
    WHERE LOAD_DATE > (SELECT MAX(LOAD_DATE) FROM {{ this }})
{% endif %}
