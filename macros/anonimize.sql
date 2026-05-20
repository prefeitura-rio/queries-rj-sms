{% macro anonimize(value_expression, namespace="'default'") %}
  TO_HEX(
    SHA256(
      CONCAT(
        '{{ var("HASH_SECRET") }}',
        '|',
        {{ namespace }},
        '|',
        {{ value_expression }}
      )
    )
  )
{% endmacro %}