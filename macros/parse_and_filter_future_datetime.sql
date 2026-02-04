{% macro parse_and_filter_future_datetime(column_name) %}
    case
        when safe_cast({{ column_name }} as datetime) > current_datetime('America/Sao_Paulo') then null
        else safe_cast({{ column_name }} as datetime)
    end
{% endmacro %}
