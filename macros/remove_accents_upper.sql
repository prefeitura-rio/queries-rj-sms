{% macro remove_accents_upper(texto) %}
    TRIM(UPPER(REGEXP_REPLACE(NORMALIZE({{ texto }}, NFD), r'\pM', '')))
{% endmacro %}
