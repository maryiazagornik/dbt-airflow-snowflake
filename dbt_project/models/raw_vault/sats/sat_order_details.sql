{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

WITH source AS (
    SELECT
        s.ORDER_PK,
        s.ORDER_DATE,
        s.ORDER_PRIORITY,
        s.SHIP_PRIORITY,
        s.CLERK,
        s.TOTAL_PRICE,
        s.ORDER_COMMENT,
        s.LOAD_DATE,
        s.RECORD_SOURCE,
        s.HASHDIFF_DETAILS AS HASHDIFF
    FROM {{ ref('stg_orders') }} AS s
),

filtered AS (
    SELECT *
    FROM source
    {% if is_incremental() %}
    WHERE LOAD_DATE > (
        SELECT COALESCE(MAX(LOAD_DATE), DATE('1900-01-01')) FROM {{ this }}
    )
    {% endif %}
),

deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_PK, HASHDIFF
        ORDER BY LOAD_DATE
    ) = 1
),

latest AS (
    SELECT ORDER_PK, HASHDIFF
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ORDER_PK
        ORDER BY LOAD_DATE DESC
    ) = 1
),

to_insert AS (
    SELECT d.*
    FROM deduped d
    LEFT JOIN latest t
      ON d.ORDER_PK = t.ORDER_PK
    WHERE t.ORDER_PK IS NULL OR d.HASHDIFF <> t.HASHDIFF
)

SELECT * FROM to_insert
