{% macro process_null(texto) %}
    nullif(
        nullif(nullif(nullif(nullif(lower({{ texto }}), 'null'), 'none'), ''), 'nat'),
        'nan'
    )
{% endmacro %}
