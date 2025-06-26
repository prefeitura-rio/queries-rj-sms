{% macro parse_date(col_name) %}
    case 
        -- ISO: 2024-06-05 ou 2024-06-05 15:55
        when REGEXP_CONTAINS({{ col_name }}, r'\d{4}-\d{2}-\d{2}')
            then SAFE_CAST(REGEXP_EXTRACT({{ col_name }}, r'\d{4}-\d{2}-\d{2}') AS DATE)

        -- Brasil: 05/06/2024 ou 05/06/2024 15:55
        when REGEXP_CONTAINS({{ col_name }}, r'\d{2}/\d{2}/\d{4}')
            then PARSE_DATE('%d/%m/%Y', REGEXP_EXTRACT({{ col_name }}, r'\d{2}/\d{2}/\d{4}'))

        else null
    end
{% endmacro %}
