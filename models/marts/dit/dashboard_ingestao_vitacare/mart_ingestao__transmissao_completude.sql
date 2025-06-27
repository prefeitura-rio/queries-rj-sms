{{ 
    config(
        alias='transmissao_completude',
        materialized='table',
    ) 
}}

WITH

  -- -----------------------------
  -- Dados Historicos
  -- -----------------------------
  dados_historicos as (
    select 
      distinct
      ut_id as source_id,
      cnes as id_cnes,
      'historico' as origem,
      safe_cast(updated_at as timestamp) as datahora_ocorrencia
    from {{ source('brutos_prontuario_vitacare_historico_staging', 'cadastro') }}
    where cnes is not null
        and safe_cast(updated_at as timestamp) between '2025-04-01' and '2025-05-01'
  ),

  -- -----------------------------
  -- Dados Continuos
  -- -----------------------------
  dados_continuos as (
    select 
      distinct
      source_id,
      nullif(json_extract_scalar(tab.data,'$.cnes'), '') AS id_cnes,
      'continuo' as origem,
      safe_cast(source_updated_at as timestamp) as datahora_ocorrencia
    from {{ source('brutos_prontuario_vitacare_staging', 'paciente_continuo') }} tab
    where nullif(json_extract_scalar(tab.data,'$.cnes'), '') is not null
        and safe_cast(source_updated_at as timestamp) between '2025-03-01' and '2025-04-01'
  ),

  -- -----------------------------
  -- Junta Dados Historicos e Continuos
  -- -----------------------------
  analise as (
    select 
      struct(
        dados_historicos.source_id,
        dados_historicos.id_cnes,
        dados_historicos.origem,
        dados_historicos.datahora_ocorrencia
      ) as historico,
      struct(
        dados_continuos.source_id,
        dados_continuos.id_cnes,
        dados_continuos.origem,
        dados_continuos.datahora_ocorrencia
      ) as continuo
    from dados_historicos
        full outer join dados_continuos using(source_id, id_cnes)
  ),

  -- -----------------------------
  -- Detecta Dados Faltantes
  -- -----------------------------
  analise_faltantes as (
    select 
      coalesce(historico.source_id, continuo.source_id) as source_id,
      coalesce(historico.id_cnes, continuo.id_cnes) as id_cnes,
      CASE 
        WHEN historico.source_id is null and continuo.source_id is not null THEN 'falta-no-historico'
        WHEN historico.source_id is not null and continuo.source_id is null THEN 'falta-no-continuo'
        ELSE 'presente-em-ambos'
      END as caso
    from analise
  )
select caso, count(*) as total
from analise_faltantes
group by 1
order by 1