{{ config(materialized='table') }}

WITH pit AS (
    SELECT p.*
    FROM {{ ref('pit_customer') }} AS p
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY p.CUSTOMER_PK
        ORDER BY p.AS_OF_DATE DESC
    ) = 1
),

hub AS (
    SELECT h.*
    FROM {{ ref('hub_customer') }} AS h
),

sat_invar AS (
    SELECT s.*
    FROM {{ ref('sat_customer_invar') }} AS s
),

sat_var AS (
    SELECT s.*
    FROM {{ ref('sat_customer_var') }} AS s
),

sat_biz_latest AS (
    SELECT s.*
    FROM {{ ref('sat_customer_business') }} AS s
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.CUSTOMER_PK
        ORDER BY s.LOAD_DATE DESC
    ) = 1
)

SELECT
    p.CUSTOMER_PK,
    h.CUSTOMER_ID,
    i.CUSTOMER_NAME,
    v.CUSTOMER_ADDRESS,
    b.MARKETING_GROUP,
    b.VIP_STATUS,
    p.AS_OF_DATE AS VALID_AS_OF_DATE
FROM pit AS p
INNER JOIN hub AS h
    ON p.CUSTOMER_PK = h.CUSTOMER_PK
LEFT JOIN sat_invar AS i
    ON
        p.CUSTOMER_PK = i.CUSTOMER_PK
        AND p.INVAR_LOAD_DATE = i.LOAD_DATE
LEFT JOIN sat_var AS v
    ON
        p.CUSTOMER_PK = v.CUSTOMER_PK
        AND p.VAR_LOAD_DATE = v.LOAD_DATE
LEFT JOIN sat_biz_latest AS b
    ON p.CUSTOMER_PK = b.CUSTOMER_PK
