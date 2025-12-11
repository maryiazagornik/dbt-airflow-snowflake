{{ config(materialized='incremental', unique_key='CUSTOMER_PK') }}

WITH raw_sat AS (
    SELECT * FROM {{ ref('sat_customer_var') }}
),

seed_data AS (
    SELECT 
        CUSTOMER_ID, 
        MARKETING_GROUP, 
        VIP_STATUS 
    FROM {{ ref('customer_marketing') }}
)

SELECT
    r.CUSTOMER_PK,
    r.LOAD_DATE,
    r.RECORD_SOURCE,
    r.CUSTOMER_ADDRESS,
    r.ACCOUNT_BALANCE,
    s.MARKETING_GROUP,
    s.VIP_STATUS,
    
    {{ hash_diff(['r.CUSTOMER_ADDRESS', 'r.ACCOUNT_BALANCE', 's.MARKETING_GROUP']) }} AS HASHDIFF_BIZ

FROM raw_sat r
LEFT JOIN {{ ref('stg_customer') }} stg ON r.CUSTOMER_PK = stg.CUSTOMER_PK
LEFT JOIN seed_data s ON stg.CUSTOMER_ID = s.CUSTOMER_ID

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} AS t
        WHERE t.CUSTOMER_PK = r.CUSTOMER_PK
          AND t.HASHDIFF_BIZ = {{ hash_diff(['r.CUSTOMER_ADDRESS', 'r.ACCOUNT_BALANCE', 's.MARKETING_GROUP']) }}
    )
{% endif %}
