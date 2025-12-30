{{ config(materialized='incremental', incremental_strategy='append') }}

WITH hub AS (
    SELECT CUSTOMER_PK
    FROM {{ ref('hub_customer') }}
),

change_dates AS (

    SELECT CUSTOMER_PK, LOAD_DATE AS AS_OF_DATE
    FROM {{ ref('sat_customer_invar') }}

    UNION

    SELECT CUSTOMER_PK, LOAD_DATE AS AS_OF_DATE
    FROM {{ ref('sat_customer_var') }}

    UNION

    SELECT CUSTOMER_PK, LOAD_DATE AS AS_OF_DATE
    FROM {{ ref('sat_customer_business') }}

    UNION

    SELECT CUSTOMER_PK, DATE('1900-01-01') AS AS_OF_DATE
    FROM hub
),

filtered_changes AS (
    SELECT
        CUSTOMER_PK,
        AS_OF_DATE
    FROM change_dates
    {% if is_incremental() %}
        WHERE AS_OF_DATE > (
            SELECT COALESCE(MAX(AS_OF_DATE), DATE('1900-01-01'))
            FROM {{ this }}
        )
    {% endif %}
),

pit_invar AS (
    SELECT
        c.CUSTOMER_PK,
        c.AS_OF_DATE,
        MAX(i.LOAD_DATE) AS SAT_INVAR_LOAD_DATE
    FROM filtered_changes AS c
    LEFT JOIN {{ ref('sat_customer_invar') }} AS i
        ON i.CUSTOMER_PK = c.CUSTOMER_PK
        AND i.LOAD_DATE <= c.AS_OF_DATE
    GROUP BY 1, 2
),

pit_var AS (
    SELECT
        c.CUSTOMER_PK,
        c.AS_OF_DATE,
        MAX(v.LOAD_DATE) AS SAT_VAR_LOAD_DATE
    FROM filtered_changes AS c
    LEFT JOIN {{ ref('sat_customer_var') }} AS v
        ON v.CUSTOMER_PK = c.CUSTOMER_PK
        AND v.LOAD_DATE <= c.AS_OF_DATE
    GROUP BY 1, 2
),

pit_biz AS (
    SELECT
        c.CUSTOMER_PK,
        c.AS_OF_DATE,
        MAX(b.LOAD_DATE) AS SAT_BIZ_LOAD_DATE
    FROM filtered_changes AS c
    LEFT JOIN {{ ref('sat_customer_business') }} AS b
        ON b.CUSTOMER_PK = c.CUSTOMER_PK
        AND b.LOAD_DATE <= c.AS_OF_DATE
    GROUP BY 1, 2
)

SELECT
    c.CUSTOMER_PK,
    c.AS_OF_DATE,

    COALESCE(i.SAT_INVAR_LOAD_DATE, DATE('1900-01-01')) AS SAT_INVAR_LOAD_DATE,
    COALESCE(v.SAT_VAR_LOAD_DATE, DATE('1900-01-01')) AS SAT_VAR_LOAD_DATE,
    COALESCE(b.SAT_BIZ_LOAD_DATE, DATE('1900-01-01')) AS SAT_BIZ_LOAD_DATE,

    CURRENT_TIMESTAMP() AS PIT_LOAD_DATE

FROM filtered_changes AS c
LEFT JOIN pit_invar AS i
    ON c.CUSTOMER_PK = i.CUSTOMER_PK
    AND c.AS_OF_DATE = i.AS_OF_DATE
LEFT JOIN pit_var AS v
    ON c.CUSTOMER_PK = v.CUSTOMER_PK
    AND c.AS_OF_DATE = v.AS_OF_DATE
LEFT JOIN pit_biz AS b
    ON c.CUSTOMER_PK = b.CUSTOMER_PK
    AND c.AS_OF_DATE = b.AS_OF_DATE

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.CUSTOMER_PK, c.AS_OF_DATE
    ORDER BY c.AS_OF_DATE
) = 1
