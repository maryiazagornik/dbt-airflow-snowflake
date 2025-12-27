{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('tpch', 'lineitem') }}
    WHERE L_SHIPDATE <= DATE('1992-01-31')
),

prepared AS (
    SELECT
        L_PARTKEY,
        L_LINENUMBER,
        L_QUANTITY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_TAX,
        L_RETURNFLAG,
        L_LINESTATUS,
        L_SHIPDATE,

        'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE,
        L_SHIPDATE AS LOAD_DATE

    FROM source
),

hashed AS (
    SELECT
        {{ hash_entity('L_PARTKEY', 'RECORD_SOURCE') }} AS PART_PK,
        L_PARTKEY AS PART_ID,

        L_LINENUMBER,
        L_QUANTITY,
        L_EXTENDEDPRICE,
        L_DISCOUNT,
        L_TAX,
        L_RETURNFLAG,
        L_LINESTATUS,

        LOAD_DATE,
        RECORD_SOURCE

    FROM prepared
)

SELECT *
FROM hashed
