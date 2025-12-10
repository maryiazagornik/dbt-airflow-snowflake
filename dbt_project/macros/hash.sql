{% macro hash_entity(field) %}
    MD5(CAST({{ field }} AS VARCHAR))
{% endmacro %}

{% macro hash_diff(fields) %}
    MD5(
        {% for field in fields %}
            CAST({{ field }} AS VARCHAR) {% if not loop.last %} || {% endif %}
        {% endfor %}
    )
{% endmacro %}
