{% macro base64_to_string(expr) %}
SAFE_CAST(
  SAFE_CONVERT_BYTES_TO_STRING(
    SAFE.FROM_BASE64({{ expr }})
  ) AS STRING
)
{% endmacro %}
