{{ config(materialized='table') }}

WITH pit AS (
    SELECT * FROM {{ ref('hub_customer') }}
),
sat_invar AS (
    SELECT * FROM {{ ref('sat_customer_invar') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_PK ORDER BY LOAD_DATE DESC) = 1
),
sat_var AS (
    SELECT * FROM {{ ref('sat_customer_var') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_PK ORDER BY LOAD_DATE DESC) = 1
),
sat_biz AS (
    SELECT * FROM {{ ref('sat_customer_business') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMER_PK ORDER BY LOAD_DATE DESC) = 1
)

SELECT
    p.CUSTOMER_PK,
    p.CUSTOMER_ID,
    i.CUSTOMER_NAME AS NAME,
    v.CUSTOMER_ADDRESS AS ADDRESS,
    b.MARKETING_GROUP
FROM pit p
LEFT JOIN sat_invar i ON p.CUSTOMER_PK = i.CUSTOMER_PK
LEFT JOIN sat_var v ON p.CUSTOMER_PK = v.CUSTOMER_PK
LEFT JOIN sat_biz b ON p.CUSTOMER_PK = b.CUSTOMER_PK
