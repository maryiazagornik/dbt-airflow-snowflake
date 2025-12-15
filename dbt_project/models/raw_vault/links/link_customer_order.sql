{{ config(materialized='incremental', unique_key='LINK_PK') }}

WITH source AS (
    SELECT
        MD5(CAST(O_ORDERKEY AS VARCHAR) || CAST(O_CUSTKEY AS VARCHAR)) AS LINK_PK,
        MD5(CAST(O_ORDERKEY AS VARCHAR)) AS ORDER_PK,
        MD5(CAST(O_CUSTKEY AS VARCHAR)) AS CUSTOMER_PK,
        O_ORDERDATE AS LOAD_DATE,
        'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE
    FROM {{ source('tpch', 'orders') }}
    WHERE O_ORDERDATE <= DATE('1992-01-31')
)

SELECT * FROM source
{% if is_incremental() %}
    WHERE LINK_PK NOT IN (SELECT t.LINK_PK FROM {{ this }} AS t)
{% endif %}
