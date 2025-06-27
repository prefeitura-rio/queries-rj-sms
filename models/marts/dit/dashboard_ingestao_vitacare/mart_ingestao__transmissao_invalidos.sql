{{ 
    config(
        alias='transmissao_problemas',
        materialized='table',
    ) 
}}

WITH
  transmissoes_individuais as (
    SELECT 
      source_id,

      nullif(payload_cnes, '') AS cnes_payload,
      nullif(json_extract_scalar(trans.data,'$.cnes'), '') AS cnes_json,

      safe_cast(TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) AS dia_ingestao,

      'paciente' as tipo_registro
    FROM {{ source('brutos_prontuario_vitacare_staging', 'paciente_continuo') }} trans

    union all

    SELECT 
      source_id,

      nullif(payload_cnes, '') AS cnes_payload,
      nullif(json_extract_scalar(trans.data,'$.unidade_cnes'), '') AS cnes_json,

      safe_cast(TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) AS dia_ingestao,

      datalake_loaded_at as momento_ingestao,

      'atendimento' as tipo_registro
    FROM {{ source('brutos_prontuario_vitacare_staging', 'atendimento_continuo') }} trans
  ),

  analise as (
    select 
      source_id,
      coalesce(cnes_payload, cnes_json, 'nao-informado') as cnes,
      momento_ingestao,

      CASE 
        WHEN cnes_payload != cnes_json THEN 'divergencia-cnes'
        WHEN cnes_payload is null or cnes_json is null THEN 'cnes-nao-informado'
        WHEN dia_ocorrencia is null THEN 'dia-ocorrencia-nao-informado'
        WHEN dia_ingestao != dia_ocorrencia THEN 'atraso-excessivo'
        ELSE null
      END as problema

    from transmissoes_individuais
    where problema is not null
  )

select *
from analise
where problema is not null
order by momento_ingestao desc