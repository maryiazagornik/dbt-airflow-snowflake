{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH source AS (
    SELECT
        s.CUSTOMER_PK,
        s.CUSTOMER_ADDRESS,
        s.CUSTOMER_PHONE,
        s.ACCOUNT_BALANCE AS ACCT_BALANCE,
        s.LOAD_DATE,
        s.RECORD_SOURCE,
        s.HASHDIFF_VAR AS HASHDIFF
    FROM {{ ref('stg_customer') }} AS s
),

filtered AS (
    SELECT *
    FROM source
    {% if is_incremental() %}
    WHERE LOAD_DATE > (
        SELECT COALESCE(MAX(LOAD_DATE), DATE('1900-01-01')) FROM {{ this }}
    )
    {% endif %}
),

deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_PK, HASHDIFF
        ORDER BY LOAD_DATE
    ) = 1
),

latest AS (
    SELECT CUSTOMER_PK, HASHDIFF
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_PK
        ORDER BY LOAD_DATE DESC
    ) = 1
),

to_insert AS (
    SELECT d.*
    FROM deduped d
    LEFT JOIN latest t
      ON d.CUSTOMER_PK = t.CUSTOMER_PK
    WHERE t.CUSTOMER_PK IS NULL OR d.HASHDIFF <> t.HASHDIFF
)

SELECT * FROM to_insert
