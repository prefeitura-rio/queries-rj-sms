{% macro json_field(data, field) %}
    json_extract_scalar(replace(data,'NaN','null'), "$['{{ field }}']")
{% endmacro %}