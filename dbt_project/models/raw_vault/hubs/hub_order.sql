{{ config(materialized='incremental', incremental_strategy='append') }}

WITH src AS (
    SELECT
        stg.ORDER_PK,
        stg.ORDER_ID,
        stg.LOAD_DATE,
        stg.RECORD_SOURCE
    FROM {{ ref('stg_orders') }} AS stg
)

SELECT DISTINCT
    src.ORDER_PK,
    src.ORDER_ID,
    src.LOAD_DATE,
    src.RECORD_SOURCE
FROM src

{% if is_incremental() %}
    WHERE src.LOAD_DATE > (
        SELECT COALESCE(MAX(t.LOAD_DATE), DATE('1900-01-01'))
        FROM {{ this }} AS t
    )
{% endif %}
