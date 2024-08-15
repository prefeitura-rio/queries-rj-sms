{% macro dict_to_json(texto) %}
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        {{ texto }},
                    '\\', '\\\\'), -- Substitui barra invertida por duas barras invertidas
                '", "', '\\"\\"'), -- Substitui aspas duplas internas por aspas escapadas
            "False", "false"),
        "True", "true"),
    "'", '"') -- Substitui aspas simples por aspas duplas
{% endmacro %}
