{{ config(materialized='view') }}

WITH customer_src AS (
    SELECT *
    FROM {{ source('tpch', 'customer') }}
),

orders_src AS (
    /*
      TPCH customer table does not contain a reliable "last_updated" timestamp.
      To support incremental patterns ("load by date"), we derive a stable LOAD_DATE
      from the earliest order date per customer.

      This makes LOAD_DATE deterministic and prevents re-loading the entire customer
      set on each run.
    */
    SELECT
        O_CUSTKEY AS CUSTOMER_ID,
        MIN(O_ORDERDATE) AS FIRST_ORDER_DATE
    FROM {{ source('tpch', 'orders') }}
    GROUP BY 1
),

prepared AS (
    SELECT
        c.*,
        'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE,
        COALESCE(o.FIRST_ORDER_DATE, DATE('1900-01-01')) AS LOAD_DATE
    FROM customer_src AS c
    LEFT JOIN orders_src AS o
        ON c.C_CUSTKEY = o.CUSTOMER_ID
),

hashed AS (
    SELECT
        {{ hash_entity('C_CUSTKEY', 'RECORD_SOURCE') }} AS CUSTOMER_PK,
        {{ hash_entity('C_NATIONKEY', 'RECORD_SOURCE') }} AS NATION_PK,

        C_CUSTKEY AS CUSTOMER_ID,
        C_NATIONKEY AS NATION_ID,

        C_NAME AS CUSTOMER_NAME,
        C_ADDRESS AS CUSTOMER_ADDRESS,
        C_PHONE AS CUSTOMER_PHONE,
        C_ACCTBAL AS ACCOUNT_BALANCE,
        C_MKTSEGMENT AS MKT_SEGMENT,

        RECORD_SOURCE,
        LOAD_DATE,

        {{ hash_diff(['C_NAME', 'C_MKTSEGMENT']) }} AS HASHDIFF_INVAR,
        {{ hash_diff(['C_ADDRESS', 'C_PHONE', 'C_ACCTBAL']) }} AS HASHDIFF_VAR

    FROM prepared
)

SELECT * FROM hashed
