{{ config(materialized='incremental', incremental_strategy='append') }}

WITH src AS (
    SELECT
        hub.CUSTOMER_PK,
        stg.LOAD_DATE,
        stg.RECORD_SOURCE,
        stg.CUSTOMER_ADDRESS,
        stg.ACCOUNT_BALANCE,
        {{ hash_diff(['stg.CUSTOMER_ADDRESS', 'stg.ACCOUNT_BALANCE']) }} AS HASHDIFF
    FROM {{ ref('hub_customer') }} AS hub
    INNER JOIN {{ ref('stg_customer') }} AS stg
        ON hub.CUSTOMER_PK = stg.CUSTOMER_PK
)

SELECT
    src.CUSTOMER_PK,
    src.LOAD_DATE,
    src.RECORD_SOURCE,
    src.CUSTOMER_ADDRESS,
    src.ACCOUNT_BALANCE,
    src.HASHDIFF
FROM src

{% if is_incremental() %}
    WHERE src.LOAD_DATE > (
        SELECT COALESCE(MAX(t.LOAD_DATE), DATE('1900-01-01'))
        FROM {{ this }} AS t
    )
{% endif %}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY src.CUSTOMER_PK, src.HASHDIFF
    ORDER BY src.LOAD_DATE
) = 1
