{% macro process_null(texto) %}
    NULLIF(
    NULLIF(
    NULLIF(
    NULLIF(
    NULLIF(
        {{ texto }}
    , 'null')
    , 'None')
    , '')
    ,'NaT')
    ,'nan')
{% endmacro %}
