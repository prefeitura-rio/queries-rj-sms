{% macro clean_numeric(texto) %}
    case
    when regexp_replace({{texto}}, '[^0-9]', '') = ''
    then null
    else regexp_replace({{texto}}, '[^0-9]', '')
    end
{% endmacro %}