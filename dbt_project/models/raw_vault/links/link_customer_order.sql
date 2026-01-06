{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        {{ hash_key(['ORDER_PK', 'CUSTOMER_PK'], 'RECORD_SOURCE') }} AS LINK_CUSTOMER_ORDER_PK,
        ORDER_PK,
        CUSTOMER_PK,
        LOAD_DATE,
        RECORD_SOURCE
    FROM {{ ref('stg_orders') }}
),

to_insert AS (
    SELECT DISTINCT
        s.LINK_CUSTOMER_ORDER_PK,
        s.ORDER_PK,
        s.CUSTOMER_PK,
        s.LOAD_DATE,
        s.RECORD_SOURCE
    FROM source s
    {% if is_incremental() %}
    LEFT JOIN {{ this }} t
      ON t.LINK_CUSTOMER_ORDER_PK = s.LINK_CUSTOMER_ORDER_PK
    WHERE t.LINK_CUSTOMER_ORDER_PK IS NULL
    {% endif %}
)

SELECT *
FROM to_insert
