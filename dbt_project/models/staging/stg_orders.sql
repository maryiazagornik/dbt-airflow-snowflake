{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('tpch', 'orders') }}
    WHERE O_ORDERDATE <= DATE('1998-01-01')
),

prepared AS (
    SELECT
        O_ORDERKEY,
        O_CUSTKEY,
        O_ORDERSTATUS,
        O_TOTALPRICE,
        O_ORDERDATE,
        O_ORDERPRIORITY,
        O_CLERK,
        O_SHIPPRIORITY,
        O_COMMENT,

        'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE,
        O_ORDERDATE AS LOAD_DATE
    FROM source
),

hashed AS (
    SELECT
        {{ hash_entity('O_ORDERKEY', 'RECORD_SOURCE') }} AS ORDER_PK,
        {{ hash_entity('O_CUSTKEY', 'RECORD_SOURCE') }} AS CUSTOMER_PK,

        O_ORDERKEY AS ORDER_ID,
        O_CUSTKEY AS CUSTOMER_ID,

        LOAD_DATE,
        RECORD_SOURCE,

        O_TOTALPRICE    AS TOTAL_PRICE,
        O_ORDERSTATUS   AS ORDER_STATUS,
        O_ORDERDATE     AS ORDER_DATE,
        O_ORDERPRIORITY AS ORDER_PRIORITY,
        O_CLERK         AS CLERK,
        O_SHIPPRIORITY  AS SHIP_PRIORITY,
        O_COMMENT       AS ORDER_COMMENT,


        {{ hash_diff([
            'O_ORDERDATE',
            'O_ORDERPRIORITY',
            'O_SHIPPRIORITY',
            'O_CLERK',
            'O_TOTALPRICE',
            'O_COMMENT'
        ]) }} AS HASHDIFF_DETAILS,

        {{ hash_diff([
            'O_ORDERSTATUS'
        ]) }} AS HASHDIFF_STATUS

    FROM prepared
)

SELECT *
FROM hashed
