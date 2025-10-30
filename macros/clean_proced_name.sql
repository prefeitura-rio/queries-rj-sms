{% macro clean_proced_name(text) %}

    upper(
        trim(
            regexp_replace(
                regexp_replace(normalize({{ text }}, nfd), r"\pM", ''),
                r'[^ 0-9A-Za-z]',
                ' '
            )
        )
    )

{% endmacro %}
