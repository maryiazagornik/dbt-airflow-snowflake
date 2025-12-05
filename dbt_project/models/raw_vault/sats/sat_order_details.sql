{{ config(materialized='incremental', unique_key='ORDER_PK') }}

WITH source AS (
    SELECT 
        MD5(CAST(O_ORDERKEY AS VARCHAR)) AS ORDER_PK,
        O_TOTALPRICE,
        O_ORDERSTATUS,
        O_ORDERDATE AS LOAD_DATE,
        'SNOWFLAKE_SAMPLE' AS RECORD_SOURCE,
        MD5(CAST(O_TOTALPRICE AS VARCHAR) || O_ORDERSTATUS) AS HASHDIFF
    FROM {{ source('tpch', 'orders') }}
    WHERE O_ORDERDATE <= DATE('1992-01-31')
)

SELECT * FROM source
{% if is_incremental() %}
WHERE HASHDIFF NOT IN (SELECT HASHDIFF FROM {{ this }})
{% endif %}