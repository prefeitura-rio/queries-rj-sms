{% macro dict_to_json(texto) %}
    REPLACE(
        REPLACE(
            REPLACE(
                {{ texto }},
                "False", "false"
            ),
            "True", "true"
        ),
        "'", '"'
    )
{% endmacro %}