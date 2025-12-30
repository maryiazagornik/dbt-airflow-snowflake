{{ config(materialized='table') }}

WITH pit AS (
    SELECT *
    FROM {{ ref('pit_customer') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_PK
        ORDER BY AS_OF_DATE DESC
    ) = 1
),

hub AS (
    SELECT *
    FROM {{ ref('hub_customer') }}
),

sat_invar AS (
    SELECT *
    FROM {{ ref('sat_customer_invar') }}
),

sat_var AS (
    SELECT *
    FROM {{ ref('sat_customer_var') }}
),

sat_biz AS (
    SELECT *
    FROM {{ ref('sat_customer_business') }}
)

SELECT
    p.CUSTOMER_PK,
    h.CUSTOMER_ID,
    i.CUSTOMER_NAME AS NAME,
    v.CUSTOMER_ADDRESS AS ADDRESS,
    b.MARKETING_GROUP,
    b.VIP_STATUS,
    p.AS_OF_DATE AS VALID_AS_OF_DATE
FROM pit AS p
INNER JOIN hub AS h ON p.CUSTOMER_PK = h.CUSTOMER_PK
LEFT JOIN sat_invar AS i
    ON
        p.CUSTOMER_PK = i.CUSTOMER_PK
        AND p.SAT_INVAR_LOAD_DATE = i.LOAD_DATE
LEFT JOIN sat_var AS v
    ON
        p.CUSTOMER_PK = v.CUSTOMER_PK
        AND p.SAT_VAR_LOAD_DATE = v.LOAD_DATE
LEFT JOIN sat_biz AS b
    ON
        p.CUSTOMER_PK = b.CUSTOMER_PK
        AND p.SAT_BIZ_LOAD_DATE = b.LOAD_DATE
