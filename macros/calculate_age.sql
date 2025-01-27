{% macro calculate_age(date_of_birth) %}
    DATE_DIFF(CURRENT_DATE(), {{ date_of_birth }}, YEAR) 
    - CASE 
        WHEN EXTRACT(MONTH FROM {{ date_of_birth }}) * 100 + EXTRACT(DAY FROM {{ date_of_birth }}) 
             > EXTRACT(MONTH FROM CURRENT_DATE()) * 100 + EXTRACT(DAY FROM CURRENT_DATE()) 
        THEN 1
        ELSE 0
      END
{% endmacro %}