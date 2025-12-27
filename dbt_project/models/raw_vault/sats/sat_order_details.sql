{{ config(materialized='incremental', incremental_strategy='append') }}

WITH src AS (
    SELECT
        hub.ORDER_PK,
        stg.LOAD_DATE,
        stg.RECORD_SOURCE,
        stg.ORDER_DATE,
        stg.TOTAL_PRICE,
        {{ hash_diff(['stg.ORDER_DATE', 'stg.TOTAL_PRICE']) }} AS HASHDIFF
    FROM {{ ref('hub_order') }} AS hub
    INNER JOIN {{ ref('stg_orders') }} AS stg
        ON hub.ORDER_PK = stg.ORDER_PK
)

SELECT
    src.ORDER_PK,
    src.LOAD_DATE,
    src.RECORD_SOURCE,
    src.ORDER_DATE,
    src.TOTAL_PRICE,
    src.HASHDIFF
FROM src

{% if is_incremental() %}
    WHERE src.LOAD_DATE > (
        SELECT COALESCE(MAX(t.LOAD_DATE), DATE('1900-01-01'))
        FROM {{ this }} AS t
    )
{% endif %}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.ORDER_PK, src.HASHDIFF
    ORDER BY src.LOAD_DATE
) = 1
