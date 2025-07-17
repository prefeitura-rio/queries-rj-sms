{% macro parse_datetime(col_name) %}
    case 
        -- 20/01/2025 21:22:13
        when REGEXP_CONTAINS({{ col_name }}, r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}')
            -- fix order to cast in YYYY-MM-DD HH:MM:SS
            then SAFE_CAST(
                SUBSTR({{ col_name }}, 7, 4) || '-' || SUBSTR({{ col_name }}, 4, 2) || '-' || SUBSTR({{ col_name }}, 1, 2) || ' ' || REGEXP_EXTRACT({{ col_name }}, r'\d{2}:\d{2}:\d{2}') || '-03:00'
             AS timestamp)

        else null
    end
{% endmacro %}
