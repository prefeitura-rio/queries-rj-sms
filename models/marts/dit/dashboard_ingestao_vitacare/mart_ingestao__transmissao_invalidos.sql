{{ 
    config(
        alias='transmissao_problemas',
        materialized='table',
    ) 
}}

with
  transmissoes_individuais as (
    select
      source_id,

      nullif(payload_cnes, '') as cnes_payload,
      nullif(json_extract_scalar(trans.data, '$.cnes'), '') as cnes_json,

      safe_cast(TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) as dia_ingestao,

      datalake_loaded_at as momento_ingestao,

      'paciente' as tipo_registro
    from {{ source('brutos_prontuario_vitacare_staging', 'paciente_continuo') }} trans

    union all

    select
      source_id,

      nullif(payload_cnes, '') as cnes_payload,
      nullif(json_extract_scalar(trans.data,'$.unidade_cnes'), '') AS cnes_json,

      safe_cast(TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as date) as dia_ocorrencia,
      safe_cast(datalake_loaded_at as date) as dia_ingestao,

      datalake_loaded_at as momento_ingestao,

      'atendimento' as tipo_registro
    from {{ source('brutos_prontuario_vitacare_staging', 'atendimento_continuo') }} trans
  ),

  analise as (
    select
      tipo_registro,
      source_id,

      coalesce(cnes_payload, cnes_json) as cnes,
      case
        when cnes_payload is null and cnes_json is null
          then 'cnes-nao-informado'
        when cnes_payload is null
          then 'sem-cnes-payload'
        when cnes_json is null
          then 'sem-cnes-json'
        when cnes_payload != cnes_json
          then 'divergencia-cnes'
        else null
      end as problema_cnes,

      dia_ingestao,
      dia_ocorrencia,
      case
        when dia_ocorrencia is null
          then null
        else DATE_DIFF(dia_ingestao, dia_ocorrencia, DAY)
      end as dias_atraso,
      case
        when dia_ocorrencia is null
          then 'dia-ocorrencia-nao-informado'
        when dia_ingestao != dia_ocorrencia
          then 'atraso-excessivo'
        else null
      end as problema_data,

      momento_ingestao

    from transmissoes_individuais
  )

select *
from analise
where problema_cnes is not null or problema_data is not null
order by momento_ingestao desc
