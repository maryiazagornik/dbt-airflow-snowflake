{{ config(materialized='incremental', incremental_strategy='append') }}

WITH raw_sat AS (
    SELECT *
    FROM {{ ref('sat_customer_var') }}
),

seed_data AS (
    SELECT
        CUSTOMER_ID,
        MARKETING_GROUP,
        VIP_STATUS
    FROM {{ ref('customer_marketing') }}
),

source AS (
    SELECT
        r.CUSTOMER_PK,
        r.LOAD_DATE,
        r.RECORD_SOURCE,
        r.CUSTOMER_ADDRESS,
        r.ACCT_BALANCE,
        s.MARKETING_GROUP,
        s.VIP_STATUS,

        {{ hash_diff([
            'r.CUSTOMER_ADDRESS',
            'r.ACCT_BALANCE',
            's.MARKETING_GROUP',
            's.VIP_STATUS'
        ]) }} AS HASHDIFF_BIZ

    FROM raw_sat AS r
    LEFT JOIN {{ ref('stg_customer') }} AS stg
        ON r.CUSTOMER_PK = stg.CUSTOMER_PK
    LEFT JOIN seed_data AS s
        ON stg.CUSTOMER_ID = s.CUSTOMER_ID
),

filtered AS (
    SELECT *
    FROM source
    {% if is_incremental() %}
    WHERE LOAD_DATE > (
        SELECT COALESCE(MAX(LOAD_DATE), DATE('1900-01-01'))
        FROM {{ this }}
    )
    {% endif %}
),

latest AS (
    SELECT
        CUSTOMER_PK,
        HASHDIFF_BIZ
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_PK
        ORDER BY LOAD_DATE DESC
    ) = 1
),

to_insert AS (
    SELECT f.*
    FROM filtered f
    LEFT JOIN latest l
      ON f.CUSTOMER_PK = l.CUSTOMER_PK
    WHERE l.CUSTOMER_PK IS NULL OR f.HASHDIFF_BIZ <> l.HASHDIFF_BIZ
)

SELECT *
FROM to_insert
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_PK, HASHDIFF_BIZ
    ORDER BY LOAD_DATE
) = 1
