{% macro extract_competencia_from_path(path_column) %}
    REGEXP_EXTRACT({{ path_column }}, r'AP\d{1,2}/(\d{4}-\d{2})')
{% endmacro %}
