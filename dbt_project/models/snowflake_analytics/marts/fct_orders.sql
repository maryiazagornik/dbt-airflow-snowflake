{{ config(materialized='incremental', unique_key='ORDER_PK') }}

SELECT
    l.ORDER_PK,
    l.CUSTOMER_PK,
    d.ORDER_DATE,
    d.TOTAL_PRICE,
    s.ORDER_STATUS
FROM {{ ref('link_customer_order') }} AS l
INNER JOIN {{ ref('sat_order_details') }} AS d
    ON l.ORDER_PK = d.ORDER_PK
LEFT JOIN {{ ref('sat_order_status') }} AS s
    ON l.ORDER_PK = s.ORDER_PK
