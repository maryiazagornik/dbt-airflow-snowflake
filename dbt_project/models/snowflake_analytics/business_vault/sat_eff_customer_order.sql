{{ config(materialized='incremental') }}

WITH source_link AS (
    SELECT
        LINK_PK,
        LOAD_DATE,
        RECORD_SOURCE
    FROM {{ ref('link_customer_order') }}
)

SELECT
    LINK_PK,

    LOAD_DATE AS START_DATE,

    TO_TIMESTAMP('9999-12-31 00:00:00') AS END_DATE,

    TRUE AS IS_CURRENT,

    RECORD_SOURCE,
    LOAD_DATE

FROM source_link

{% if is_incremental() %}
    WHERE LOAD_DATE > (SELECT MAX(LOAD_DATE) FROM {{ this }})
{% endif %}
