{{ config(materialized='incremental', unique_key='LINK_CUSTOMER_ORDER_PK') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['ORDER_PK', 'CUSTOMER_PK']) }} as LINK_CUSTOMER_ORDER_PK,
    ORDER_PK,
    CUSTOMER_PK,
    LOAD_DATE,
    RECORD_SOURCE
FROM {{ ref('stg_orders') }}
