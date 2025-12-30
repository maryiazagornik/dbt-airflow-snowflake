{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        {{ hash_key(['ORDER_PK', 'CUSTOMER_PK'], 'RECORD_SOURCE') }} AS LINK_CUSTOMER_ORDER_PK,
        ORDER_PK,
        CUSTOMER_PK,
        LOAD_DATE,
        RECORD_SOURCE
    FROM {{ ref('stg_orders') }}
)

SELECT DISTINCT
    LINK_CUSTOMER_ORDER_PK,
    ORDER_PK,
    CUSTOMER_PK,
    LOAD_DATE,
    RECORD_SOURCE
FROM source

{% if is_incremental() %}
    WHERE LOAD_DATE > (
        SELECT COALESCE(MAX(LOAD_DATE), DATE('1900-01-01'))
        FROM {{ this }}
    )
{% endif %}
