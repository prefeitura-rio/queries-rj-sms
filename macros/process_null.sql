{% macro process_null(texto) %}
    CASE
        WHEN LOWER(TRIM({{ texto }})) IN ('null', 'none', '', 'nat', 'nan', 'na','-')
        THEN NULL
        ELSE {{ texto }}
    END
{% endmacro %}
