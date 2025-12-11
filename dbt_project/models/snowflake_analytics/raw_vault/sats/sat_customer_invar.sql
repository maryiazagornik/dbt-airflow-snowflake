{{ config(materialized='incremental', unique_key='CUSTOMER_PK') }}

WITH source AS (
    SELECT
        CUSTOMER_PK,
        CUSTOMER_NAME,
        MKT_SEGMENT,
        LOAD_DATE,
        RECORD_SOURCE,
        HASHDIFF_INVAR AS HASHDIFF
    FROM {{ ref('stg_customer') }}
)

SELECT * FROM source
{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} AS t
        WHERE t.CUSTOMER_PK = source.CUSTOMER_PK
          AND t.HASHDIFF = source.HASHDIFF
    )
{% endif %}
