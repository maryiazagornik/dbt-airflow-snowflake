{{ config(materialized='view') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['L_PARTKEY']) }} as PART_PK,
    L_PARTKEY AS PART_ID,
    L_LINENUMBER,
    L_QUANTITY,
    L_EXTENDEDPRICE,
    L_DISCOUNT,
    L_TAX,
    L_RETURNFLAG,
    L_LINESTATUS,
    L_SHIPDATE AS LOAD_DATE,
    'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE
FROM {{ source('tpch', 'lineitem') }}
WHERE L_SHIPDATE <= DATE('1992-01-31')
