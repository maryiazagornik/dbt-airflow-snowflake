{{
    config(
        materialized='incremental',
        unique_key='LINK_CUSTOMER_ORDER_PK',
        incremental_strategy='merge'
    )
}}

WITH src AS (
    SELECT
        CUSTOMER_PK,
        ORDER_PK,
        LOAD_DATE,
        RECORD_SOURCE
    FROM {{ ref('stg_orders') }}
)

SELECT DISTINCT
    MD5(CONCAT_WS('||', s.CUSTOMER_PK, s.ORDER_PK)) AS LINK_CUSTOMER_ORDER_PK,
    s.CUSTOMER_PK,
    s.ORDER_PK,
    s.LOAD_DATE,
    s.RECORD_SOURCE
FROM src AS s
