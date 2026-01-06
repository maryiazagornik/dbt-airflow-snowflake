{% macro dv_normalize(field) %}

    COALESCE(NULLIF(TRIM(CAST({{ field }} AS VARCHAR)), ''), '∅')
{% endmacro %}

{% macro hash_entity(field, record_source) %}

    MD5(
        UPPER({{ dv_normalize(field) }})
        || '|' ||
        UPPER({{ dv_normalize(record_source) }})
    )
{% endmacro %}

{% macro hash_key(fields, record_source) %}

    MD5(
        {% for field in fields %}
            UPPER({{ dv_normalize(field) }}) || '|' ||
        {% endfor %}
        UPPER({{ dv_normalize(record_source) }})
    )
{% endmacro %}

{% macro hash_diff(fields) %}

    MD5(
        {% for field in fields %}
            UPPER({{ dv_normalize(field) }}){% if not loop.last %} || '|' || {% endif %}
        {% endfor %}
    )
{% endmacro %}
