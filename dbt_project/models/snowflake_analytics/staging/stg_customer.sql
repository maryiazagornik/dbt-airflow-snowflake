{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('tpch', 'customer') }}
),

hashed AS (
    SELECT
        {{ hash_entity('C_CUSTKEY') }} AS CUSTOMER_PK,
        {{ hash_entity('C_NATIONKEY') }} AS NATION_PK,
        C_CUSTKEY AS CUSTOMER_ID,
        C_NATIONKEY AS NATION_ID,

        C_NAME AS CUSTOMER_NAME,
        C_ADDRESS AS CUSTOMER_ADDRESS,
        C_PHONE AS CUSTOMER_PHONE,
        C_ACCTBAL AS ACCOUNT_BALANCE,
        C_MKTSEGMENT AS MKT_SEGMENT,

        'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE,
        CURRENT_TIMESTAMP() AS LOAD_DATE,

        {{ hash_diff(['C_NAME']) }} AS HASHDIFF_INVAR,
        {{ hash_diff(['C_ADDRESS', 'C_PHONE', 'C_ACCTBAL']) }} AS HASHDIFF_VAR

    FROM source
)

SELECT * FROM hashed
