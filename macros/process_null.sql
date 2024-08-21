{% macro process_null(texto) %}
    NULLIF(
    NULLIF(
    NULLIF(
        {{ texto }}
    , 'null')
    , 'None')
    , '')
{% endmacro %}
