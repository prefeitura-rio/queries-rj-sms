{% macro json_field(data, field) %}
    json_extract_scalar(replace(data,'NaN','null'), "$['{{ field }}']")
{% endmacro %}

{% macro json_fields(fields, data='json') %}
    {%- for field in fields %}
        cast(
            nullif(
                json_extract_scalar(replace({{ data }}, 'NaN', 'null'), "$['{{ field }}']"),
                ''
            )
            as string
        ) as {{ adapter.quote(field) }}{% if not loop.last %},{% endif %}
    {%- endfor %}
{% endmacro %}