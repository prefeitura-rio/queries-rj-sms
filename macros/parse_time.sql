{% macro parse_time(col_name) %}
    case
        when REGEXP_CONTAINS(
            LPAD({{ col_name }},4,'0'), 
            r'\d{2}\d{2}'
            )
            then TIME(
                CAST(SUBSTR(LPAD({{ col_name }},4,'0'), 1, 2) AS INT64), 
                CAST(SUBSTR(LPAD({{ col_name }},4,'0'), 3, 2) AS INT64),
                0
            )
        else null
    end
{% endmacro %}
