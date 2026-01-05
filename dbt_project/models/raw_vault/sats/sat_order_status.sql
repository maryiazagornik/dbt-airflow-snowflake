{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH source AS (
    SELECT
        ORDER_PK,
        LOAD_DATE,
        RECORD_SOURCE,

        ORDER_STATUS,

        {{ hash_diff([
            'ORDER_STATUS'
        ]) }} AS HASHDIFF

    FROM {{ ref('stg_orders') }}
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

, latest AS (
    SELECT
        ORDER_PK,
        HASHDIFF
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_PK
        ORDER BY LOAD_DATE DESC
    ) = 1
),

to_insert AS (
    SELECT f.*
    FROM filtered f
    LEFT JOIN latest l
        ON f.ORDER_PK = l.ORDER_PK
    WHERE l.ORDER_PK IS NULL
       OR f.HASHDIFF <> l.HASHDIFF
)

SELECT *
FROM to_insert
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ORDER_PK, HASHDIFF
    ORDER BY LOAD_DATE
) = 1

{% else %}

SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ORDER_PK, HASHDIFF
    ORDER BY LOAD_DATE
) = 1

{% endif %}
