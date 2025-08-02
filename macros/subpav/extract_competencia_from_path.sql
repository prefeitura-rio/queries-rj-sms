{% macro extract_competencia_from_path(path_column) %}
    case
        when {{ path_column }} is null then null
        when regexp_contains(lower({{ path_column }}), r'ap\d{1,2}/\d{4}-\d{2}') then
            regexp_extract(lower({{ path_column }}), r'ap\d{1,2}/(\d{4}-\d{2})')
        else null
    end
{% endmacro %}
