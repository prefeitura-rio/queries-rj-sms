{% macro random_int(value_expression, max_value, namespace="'default'") %}
  {#-
    Gera um número inteiro determinístico entre -max_value e max_value
    usando FARM_FINGERPRINT com hash secreto.

    Exemplos:
      {{ random_int('paciente_id', 100, "'hackathon'") }}  -- retorna -100 a 100
      {{ random_int('cpf', 50) }}                          -- retorna -50 a 50
  -#}
  (
    MOD(
      ABS(
        FARM_FINGERPRINT(
          CONCAT(
            '{{ var("HASH_SECRET") }}',
            '|',
            {{ namespace }},
            '|',
            CAST({{ value_expression }} AS STRING)
          )
        )
      ),
      {{ max_value }} * 2 + 1
    ) - {{ max_value }}
  )
{% endmacro %}
