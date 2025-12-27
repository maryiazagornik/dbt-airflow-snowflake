{{ config(materialized='incremental', incremental_strategy='append') }}

WITH link AS (
    SELECT l.*
    FROM {{ ref('link_customer_order') }} AS l
),

sat_details AS (
    SELECT d.*
    FROM {{ ref('sat_order_details') }} AS d
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY d.ORDER_PK
        ORDER BY d.LOAD_DATE DESC
    ) = 1
),

sat_status AS (
    SELECT s.*
    FROM {{ ref('sat_order_status') }} AS s
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.ORDER_PK
        ORDER BY s.LOAD_DATE DESC
    ) = 1
)

SELECT
    l.ORDER_PK,
    l.CUSTOMER_PK,
    d.ORDER_DATE,
    d.TOTAL_PRICE,
    s.ORDER_STATUS,
    l.LOAD_DATE,
    l.RECORD_SOURCE
FROM link AS l
INNER JOIN sat_details AS d
    ON l.ORDER_PK = d.ORDER_PK
LEFT JOIN sat_status AS s
    ON l.ORDER_PK = s.ORDER_PK

{% if is_incremental() %}
    WHERE l.LOAD_DATE > (
        SELECT COALESCE(MAX(t.LOAD_DATE), DATE('1900-01-01'))
        FROM {{ this }} AS t
    )
{% endif %}
