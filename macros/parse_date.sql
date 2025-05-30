{% macro parse_date(col_name) %}
    case 
        when REGEXP_CONTAINS({{col_name}}, r'\d{4}-\d{2}-\d{2}')
            then cast({{ col_name }} as date format 'YYYY-MM-DD')
        when REGEXP_CONTAINS({{col_name}}, r'\d{2}/\d{2}/\d{4}')
            then cast({{ col_name }} as date format 'DD/MM/YYYY')
        else null
    end
{% endmacro %}
