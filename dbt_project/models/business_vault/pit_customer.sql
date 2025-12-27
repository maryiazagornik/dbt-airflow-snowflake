{{ config(materialized='table') }}

WITH as_of AS (
    SELECT DISTINCT s.LOAD_DATE::DATE AS AS_OF_DATE
    FROM {{ ref('sat_customer_invar') }} AS s

    UNION ALL

    SELECT DISTINCT s.LOAD_DATE::DATE AS AS_OF_DATE
    FROM {{ ref('sat_customer_var') }} AS s

    UNION ALL

    SELECT DISTINCT s.LOAD_DATE::DATE AS AS_OF_DATE
    FROM {{ ref('sat_customer_business') }} AS s
),

customers AS (
    SELECT DISTINCT h.CUSTOMER_PK
    FROM {{ ref('hub_customer') }} AS h
),

grid AS (
    SELECT
        c.CUSTOMER_PK,
        a.AS_OF_DATE
    FROM customers AS c
    CROSS JOIN as_of AS a
),

invar_ranked AS (
    SELECT
        g.CUSTOMER_PK,
        g.AS_OF_DATE,
        s.LOAD_DATE AS SAT_LOAD_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY g.CUSTOMER_PK, g.AS_OF_DATE
            ORDER BY s.LOAD_DATE DESC
        ) AS RN
    FROM grid AS g
    LEFT JOIN {{ ref('sat_customer_invar') }} AS s
        ON
            g.CUSTOMER_PK = s.CUSTOMER_PK
            AND s.LOAD_DATE::DATE <= g.AS_OF_DATE
),

var_ranked AS (
    SELECT
        g.CUSTOMER_PK,
        g.AS_OF_DATE,
        s.LOAD_DATE AS SAT_LOAD_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY g.CUSTOMER_PK, g.AS_OF_DATE
            ORDER BY s.LOAD_DATE DESC
        ) AS RN
    FROM grid AS g
    LEFT JOIN {{ ref('sat_customer_var') }} AS s
        ON
            g.CUSTOMER_PK = s.CUSTOMER_PK
            AND s.LOAD_DATE::DATE <= g.AS_OF_DATE
),

biz_ranked AS (
    SELECT
        g.CUSTOMER_PK,
        g.AS_OF_DATE,
        s.LOAD_DATE AS SAT_LOAD_DATE,
        ROW_NUMBER() OVER (
            PARTITION BY g.CUSTOMER_PK, g.AS_OF_DATE
            ORDER BY s.LOAD_DATE DESC
        ) AS RN
    FROM grid AS g
    LEFT JOIN {{ ref('sat_customer_business') }} AS s
        ON
            g.CUSTOMER_PK = s.CUSTOMER_PK
            AND s.LOAD_DATE::DATE <= g.AS_OF_DATE
),

final AS (
    SELECT
        g.CUSTOMER_PK,
        g.AS_OF_DATE,
        iv.SAT_LOAD_DATE AS INVAR_LOAD_DATE,
        vv.SAT_LOAD_DATE AS VAR_LOAD_DATE,
        bz.SAT_LOAD_DATE AS BIZ_LOAD_DATE,
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM grid AS g
    LEFT JOIN invar_ranked AS iv
        ON
            g.CUSTOMER_PK = iv.CUSTOMER_PK
            AND g.AS_OF_DATE = iv.AS_OF_DATE
            AND iv.RN = 1
    LEFT JOIN var_ranked AS vv
        ON
            g.CUSTOMER_PK = vv.CUSTOMER_PK
            AND g.AS_OF_DATE = vv.AS_OF_DATE
            AND vv.RN = 1
    LEFT JOIN biz_ranked AS bz
        ON
            g.CUSTOMER_PK = bz.CUSTOMER_PK
            AND g.AS_OF_DATE = bz.AS_OF_DATE
            AND bz.RN = 1
)

SELECT f.*
FROM final AS f
