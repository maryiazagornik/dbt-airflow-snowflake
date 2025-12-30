{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        CUSTOMER_PK,
        CUSTOMER_ID,
        LOAD_DATE,
        RECORD_SOURCE
    FROM {{ ref('stg_customer') }}
)

SELECT DISTINCT
    CUSTOMER_PK,
    CUSTOMER_ID,
    LOAD_DATE,
    RECORD_SOURCE
FROM source

{% if is_incremental() %}
    WHERE LOAD_DATE > (
        SELECT COALESCE(MAX(LOAD_DATE), DATE('1900-01-01'))
        FROM {{ this }}
    )
{% endif %}
