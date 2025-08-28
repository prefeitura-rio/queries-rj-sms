{{ config(
    materialized = "incremental",
    schema = "intermediario_sisreg",
    alias = "procedimentos"
) }}

WITH procedimentos AS (
    SELECT DISTINCT
      codigo_interno_procedimento,
      descricao_interna_procedimento,
      codigo_sigtap_procedimento,
      descricao_sigtap_procedimento,
      CAST(NULL AS STRING) AS procedimento_padronizado
    FROM {{ source("brutos_sisreg_api_staging", "marcacoes") }}
)

SELECT *
FROM procedimentos

{% if is_incremental() %}
  WHERE codigo_interno_procedimento NOT IN (
    SELECT DISTINCT codigo_interno_procedimento
    FROM {{ this }}
  )
{% endif %}
