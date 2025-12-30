{{ config(materialized='incremental', incremental_strategy='append') }}

WITH link AS (
    SELECT *
    FROM {{ ref('link_customer_order') }}
),

sat_details AS (
    SELECT *
    FROM {{ ref('sat_order_details') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_PK
        ORDER BY LOAD_DATE DESC
    ) = 1
),

sat_status AS (
    SELECT *
    FROM {{ ref('sat_order_status') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_PK
        ORDER BY LOAD_DATE DESC
    ) = 1
)

SELECT
    l.ORDER_PK,
    l.CUSTOMER_PK,
    d.ORDER_DATE,
    d.TOTAL_PRICE,
    s.ORDER_STATUS
FROM link AS l
INNER JOIN sat_details AS d
    ON l.ORDER_PK = d.ORDER_PK
LEFT JOIN sat_status AS s
    ON l.ORDER_PK = s.ORDER_PK

{% if is_incremental() %}
    WHERE l.LOAD_DATE > (
        SELECT COALESCE(MAX(ORDER_DATE), DATE('1900-01-01'))
        FROM {{ this }}
    )
{% endif %}
