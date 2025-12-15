{{ config(materialized='table') }}

WITH parts AS (
    SELECT
        PART_ID,
        MIN(L_EXTENDEDPRICE) AS min_price,
        MAX(L_EXTENDEDPRICE) AS max_price,
        AVG(L_EXTENDEDPRICE) AS avg_price
    FROM {{ ref('stg_lineitem') }}
    GROUP BY PART_ID
)

SELECT
    PART_ID,
    min_price,
    max_price,
    avg_price
FROM parts
