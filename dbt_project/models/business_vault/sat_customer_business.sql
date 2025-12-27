{{ config(materialized="incremental") }}

WITH src AS (
    SELECT
        sat.customer_pk,
        sat.load_date,
        sat.record_source,
        sat.customer_id,
        sat.first_name,
        sat.last_name,
        sat.email,
        sat.phone
    FROM {{ ref("sat_customer_var") }} AS sat
),

marketing AS (
    SELECT
        m.customer_id,
        m.segment,
        m.channel,
        m.campaign
    FROM {{ ref("customer_marketing") }} AS m
),

final AS (
    SELECT
        src.customer_pk,
        src.load_date,
        src.record_source,
        {{ hashdiff_sat([
            "src.customer_id",
            "src.first_name",
            "src.last_name",
            "src.email",
            "src.phone",
            "marketing.segment",
            "marketing.channel",
            "marketing.campaign",
        ]) }} AS hashdiff,
        src.customer_id,
        src.first_name,
        src.last_name,
        src.email,
        src.phone,
        marketing.segment,
        marketing.channel,
        marketing.campaign
    FROM src
    LEFT JOIN marketing
        ON src.customer_id = marketing.customer_id
)

SELECT
    final.customer_pk,
    final.hashdiff,
    final.load_date,
    final.record_source,
    final.customer_id,
    final.first_name,
    final.last_name,
    final.email,
    final.phone,
    final.segment,
    final.channel,
    final.campaign
FROM final
