{{ config(materialized='incremental', unique_key='CUSTOMER_PK') }}

SELECT DISTINCT
    source.CUSTOMER_PK,
    source.CUSTOMER_ID,
    source.LOAD_DATE,
    source.RECORD_SOURCE
FROM {{ ref('stg_customer') }} AS source

{% if is_incremental() %}
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ this }} AS target
        WHERE target.CUSTOMER_PK = source.CUSTOMER_PK
    )
{% endif %}
