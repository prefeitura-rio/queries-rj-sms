{% macro normalize_null(value) %}
case
    when {{ value }} is null then null
    when (
        lower(trim(cast({{ value }} as string)))
    ) in ('null', 'none', 'na', 'n/a', 'nan', 'nat', '', '-')
    then null
    else {{ value }}
end
{% endmacro %}
