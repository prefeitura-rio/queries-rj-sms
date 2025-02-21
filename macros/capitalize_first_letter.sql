{% macro capitalize_first_letter(text) %}
    concat(
        upper(
            substring(
                {{text}},
                1,
                1
            )
        ),
        lower(
            substring(
                {{text}}, 
                2,
                char_length({{text}})
            )
        )
    )
{% endmacro %}
