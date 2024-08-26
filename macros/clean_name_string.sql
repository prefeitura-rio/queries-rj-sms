{% macro clean_name_string(text) %}

    upper(
        trim(
            regexp_replace(
                regexp_replace(normalize({{ text}}, nfd), r"\pM", ''), r'[^ A-Z-a-z]', ' '
            )
        )
    )

{% endmacro %}
