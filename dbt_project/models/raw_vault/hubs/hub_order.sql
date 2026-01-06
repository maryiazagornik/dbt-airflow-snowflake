{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        ORDER_PK,
        ORDER_ID,
        LOAD_DATE,
        RECORD_SOURCE
    FROM {{ ref('stg_orders') }}
),

to_insert AS (
    SELECT DISTINCT
        s.ORDER_PK,
        s.ORDER_ID,
        s.LOAD_DATE,
        s.RECORD_SOURCE
    FROM source s
    {% if is_incremental() %}
    LEFT JOIN {{ this }} t
      ON t.ORDER_PK = s.ORDER_PK
    WHERE t.ORDER_PK IS NULL
    {% endif %}
)

SELECT *
FROM to_insert
