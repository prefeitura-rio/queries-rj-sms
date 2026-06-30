{% macro proper_vacina_dose(val) %}
case
  when lower(trim({{ val }})) in (
    "outro", "outra", "dose"
  )
    then null
  else
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                {{ capitalize_first_letter("vacina_dose") }},
                "eforco",  -- ex. "Reforco"
                "eforço"
              ),
              "acinacao", -- ex. "Revacinacao"
              "acinação"
            ),
            "unica", -- ex. "Dose unica"
            "única"
          ),
          r"([0-9]+)\s*dose",  -- ex. "2 dose" -> "2ª dose"
          r"\1ª dose"
        ),
        r"([0-9]+)\s*reforço",  -- ex. "2 reforço" -> "2º reforço"
        r"\1º reforço"
      ),
      r"^Unica$", -- SI-PNI tem "unica" ao invés de "dose unica"
      "Dose única"
    )
end
{% endmacro %}
