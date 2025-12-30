{#
  DV2 hashing helpers (Snowflake)

  Requirements from mentor:
  - record_source MUST be included in hash keys for Hubs and Links.
  - Raw Vault should be insert-only, so stable hashing matters to avoid duplicates.

  Implementation notes:
  - Normalise all values (trim + cast) and replace NULL/empty with a placeholder.
  - Use UPPER() to avoid case-related hash changes.
  - Use a clear delimiter between fields.
#}

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
