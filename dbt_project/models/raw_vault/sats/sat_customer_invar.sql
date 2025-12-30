{{ config(materialized='incremental', incremental_strategy='append') }}

WITH source AS (
    SELECT
        CUSTOMER_PK,
        CUSTOMER_NAME,
        MKT_SEGMENT,
        LOAD_DATE,
        RECORD_SOURCE,
        HASHDIFF_INVAR AS HASHDIFF
    FROM {{ ref('stg_customer') }}
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
)

{% if is_incremental() %}
,
latest AS (
    SELECT
        CUSTOMER_PK,
        HASHDIFF
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMER_PK
        ORDER BY LOAD_DATE DESC
    ) = 1
),

to_insert AS (
    SELECT
        f.*
    FROM filtered AS f
    LEFT JOIN latest AS l
        ON f.CUSTOMER_PK = l.CUSTOMER_PK
    WHERE l.CUSTOMER_PK IS NULL OR f.HASHDIFF <> l.HASHDIFF
)

SELECT *
FROM to_insert
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_PK, HASHDIFF
    ORDER BY LOAD_DATE
) = 1
{% else %}

SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_PK, HASHDIFF
    ORDER BY LOAD_DATE
) = 1

{% endif %}
