{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['LINK_CUSTOMER_ORDER_PK', 'START_DATE'],
        on_schema_change='sync_all_columns'
    )
}}

WITH link_events AS (
    SELECT
        LINK_CUSTOMER_ORDER_PK,
        LOAD_DATE::DATE AS START_DATE,
        RECORD_SOURCE
    FROM {{ ref('link_customer_order') }}
)

{% if is_incremental() %}
,
new_events AS (
    SELECT *
    FROM link_events
    WHERE START_DATE > (
        SELECT COALESCE(MAX(START_DATE), DATE('1900-01-01'))
        FROM {{ this }}
    )
),

affected_links AS (
    SELECT DISTINCT LINK_CUSTOMER_ORDER_PK
    FROM new_events
),

current_open AS (
    SELECT
        LINK_CUSTOMER_ORDER_PK,
        START_DATE,
        RECORD_SOURCE
    FROM {{ this }}
    WHERE IS_CURRENT = TRUE
      AND LINK_CUSTOMER_ORDER_PK IN (SELECT LINK_CUSTOMER_ORDER_PK FROM affected_links)
),

events AS (
    SELECT * FROM current_open
    UNION ALL
    SELECT * FROM new_events
)
{% else %}
,
events AS (
    SELECT * FROM link_events
)
{% endif %}
,
final AS (
    SELECT
        LINK_CUSTOMER_ORDER_PK,
        START_DATE,

        COALESCE(
            LEAD(START_DATE) OVER (
                PARTITION BY LINK_CUSTOMER_ORDER_PK
                ORDER BY START_DATE
            ),
            DATE('9999-12-31')
        ) AS END_DATE,

        CASE
            WHEN LEAD(START_DATE) OVER (
                PARTITION BY LINK_CUSTOMER_ORDER_PK
                ORDER BY START_DATE
            ) IS NULL THEN TRUE
            ELSE FALSE
        END AS IS_CURRENT,

        RECORD_SOURCE,
        CURRENT_TIMESTAMP() AS LOAD_DATE
    FROM events
)

SELECT * FROM final
