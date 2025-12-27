{{ config(materialized="incremental") }}

WITH link AS (
    SELECT
        l.link_customer_order_pk,
        l.customer_pk,
        l.order_pk,
        l.load_date AS link_load_date,
        l.record_source AS link_record_source
    FROM {{ ref("link_customer_order") }} AS l
),

order_status AS (
    SELECT
        s.order_pk,
        s.load_date AS status_load_date,
        s.record_source AS status_record_source,
        s.start_date,
        s.end_date,
        s.order_status
    FROM {{ ref("sat_order_status") }} AS s
),

final AS (
    SELECT
        link.link_customer_order_pk,
        link.customer_pk,
        link.order_pk,
        order_status.start_date,
        order_status.end_date,
        order_status.order_status,
        GREATEST(link.link_load_date, order_status.status_load_date) AS load_date,
        COALESCE(order_status.status_record_source, link.link_record_source) AS record_source,
        {{ hashdiff_sat([
            "link.link_customer_order_pk",
            "link.customer_pk",
            "link.order_pk",
            "order_status.start_date",
            "order_status.end_date",
            "order_status.order_status",
        ]) }} AS hashdiff
    FROM link
    INNER JOIN order_status
        ON link.order_pk = order_status.order_pk
)

SELECT
    final.link_customer_order_pk,
    final.hashdiff,
    final.load_date,
    final.record_source,
    final.customer_pk,
    final.order_pk,
    final.start_date,
    final.end_date,
    final.order_status
FROM final
