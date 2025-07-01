{{ 
    config(
        alias='transmissao_completude',
        materialized='table',
    ) 
}}

with

  -- -----------------------------
  -- Dados Históricos
  -- -----------------------------
  dados_historicos as (
    select distinct
      ut_id as source_id,
      cnes as id_cnes,
      'historico' as origem,
      format_datetime('%Y-%m', datetime(updated_at)) as mes
    from {{ source('brutos_prontuario_vitacare_historico_staging', 'cadastro') }}
    where cnes is not null
      -- Pega somente ocorrências dos últimos 6 meses
      and datetime(updated_at) >
        -- [Ref] https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions
        -- Transforma dia em 01 (i.e. conta a partir do dia 1º de 6 meses atrás)
        date_trunc(
          -- Subtrai 6 meses do dia em que o código está sendo executado
          date(
            datetime_sub(
              current_datetime("America/Sao_Paulo"),
              interval 6 month
            )
          ),
          month
        )
  ),

  -- -----------------------------
  -- Dados Contínuos
  -- -----------------------------
  dados_continuos as (
    select distinct
      source_id,
      nullif(json_extract_scalar(tab.data,'$.cnes'), '') as id_cnes,
      'continuo' as origem,
      format_datetime('%Y-%m', datetime(source_updated_at)) as mes
    from {{ source('brutos_prontuario_vitacare_staging', 'paciente_continuo') }} as tab
    where nullif(json_extract_scalar(tab.data,'$.cnes'), '') is not null
      and datetime(source_updated_at) >
        date_trunc(
          date(
            datetime_sub(
              current_datetime("America/Sao_Paulo"),
              interval 6 month
            )
          ),
          month
        )
  ),

  -- -----------------------------
  -- Junta Históricos e Contínuos
  -- -----------------------------
  analise as (
    select 
      struct(
        dados_historicos.source_id,
        dados_historicos.id_cnes,
        dados_historicos.origem,
        dados_historicos.mes
      ) as historico,
      struct(
        dados_continuos.source_id,
        dados_continuos.id_cnes,
        dados_continuos.origem,
        dados_continuos.mes
      ) as continuo
    from dados_historicos
      full outer join dados_continuos using(mes, source_id, id_cnes)
    -- IDs de cadastro são únicos somente no próprio CNES; portanto
    -- o que (deve ser) único globalmente é o par ID + CNES
    -- Também queremos ocorrências no mesmo mês
  ),

  -- -----------------------------
  -- Detecta Dados Faltantes
  -- -----------------------------
  analise_faltantes as (
    select
      coalesce(historico.mes, continuo.mes) as mes,
      coalesce(historico.source_id, continuo.source_id) as source_id,
      coalesce(historico.id_cnes, continuo.id_cnes) as id_cnes,
      case
        when historico.source_id is null and continuo.source_id is not null
          then 'falta-no-historico'
        when historico.source_id is not null and continuo.source_id is null
          then 'falta-no-continuo'
        else 'presente-em-ambos'
      end as caso
    from analise
  )

select distinct mes, id_cnes, source_id,
  countif(caso = 'presente-em-ambos') as presente_em_ambos,
  countif(caso = 'falta-no-continuo') as falta_continuo,
  countif(caso = 'falta-no-historico') as falta_historico
from analise_faltantes
group by mes, id_cnes, source_id
order by mes desc, id_cnes desc


-- select distinct mes, id_cnes,
--   sum(presente_em_ambos) as presente_em_ambos,
--   sum(falta_continuo) as falta_continuo,
--   sum(falta_historico) as falta_historico
-- from `...`
-- where mes = '2025-05'
-- group by mes, id_cnes
-- order by mes desc

