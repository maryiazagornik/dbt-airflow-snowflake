{% macro dv_normalize(field) -%}
    COALESCE(NULLIF(TRIM(CAST({{ field }} AS STRING)), ''), '∅')
{%- endmacro %}

{% macro hash_key(fields) -%}
MD5(
    CONCAT_WS(
        '||',
    {%- for f in fields -%}
        {{ dv_normalize(f) }}{% if not loop.last %},{% endif %}
    {%- endfor -%}
    )
)
{%- endmacro %}

{# Hashdiff for Satellites (NO record_source required) #}
{% macro hashdiff_sat(fields) -%}
    {{ hash_key(fields) }}
{%- endmacro %}
