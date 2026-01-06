{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        CUSTOMER_PK,
        CUSTOMER_ID,
        LOAD_DATE,
        RECORD_SOURCE
    FROM {{ ref('stg_customer') }}
),

to_insert AS (
    SELECT DISTINCT
        s.CUSTOMER_PK,
        s.CUSTOMER_ID,
        s.LOAD_DATE,
        s.RECORD_SOURCE
    FROM source s
    {% if is_incremental() %}
    LEFT JOIN {{ this }} t
      ON t.CUSTOMER_PK = s.CUSTOMER_PK
    WHERE t.CUSTOMER_PK IS NULL
    {% endif %}
)

SELECT *
FROM to_insert
