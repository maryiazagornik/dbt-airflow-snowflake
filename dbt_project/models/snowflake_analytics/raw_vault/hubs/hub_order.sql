{{ config(
    materialized='incremental',
    unique_key=['ORDER_PK', 'LOAD_DATE', 'RECORD_SOURCE'],
    incremental_strategy='merge',
    tags=['raw_vault', 'hub']
) }}

with src as (
    select
        ORDER_ID as BK_ORDER_ID,
        RECORD_SOURCE,
        LOAD_DATE,
        sha2(coalesce(to_varchar(ORDER_ID), ''), 256) as ORDER_PK
    from {{ ref('stg_orders') }}
)

select * from src

{% if is_incremental() %}
    where LOAD_DATE > (
        select coalesce(max(t.LOAD_DATE), '1900-01-01'::date)
        from {{ this }} as t
    )
{% endif %}
