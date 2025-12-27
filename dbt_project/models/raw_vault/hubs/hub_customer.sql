{{ config(materialized='incremental', incremental_strategy='append') }}

WITH src AS (
    SELECT
        stg.CUSTOMER_PK,
        stg.CUSTOMER_ID,
        stg.LOAD_DATE,
        stg.RECORD_SOURCE
    FROM {{ ref('stg_customer') }} AS stg
)

SELECT DISTINCT
    src.CUSTOMER_PK,
    src.CUSTOMER_ID,
    src.LOAD_DATE,
    src.RECORD_SOURCE
FROM src

{% if is_incremental() %}
    WHERE src.LOAD_DATE > (
        SELECT COALESCE(MAX(t.LOAD_DATE), DATE('1900-01-01'))
        FROM {{ this }} AS t
    )
{% endif %}
