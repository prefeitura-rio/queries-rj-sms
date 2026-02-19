{% macro parse_and_filter_future_date(column_name) %}
    case 
        when safe_cast({{ column_name }} as date) > current_date('America/Sao_Paulo') 
            then null
        else 
            safe_cast({{ column_name }} as date)
    end
{% endmacro %}