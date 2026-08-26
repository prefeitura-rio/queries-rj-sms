{{
    config(
        alias='serie_temporal_ingestao_pcsm',
        materialized='incremental',
        incremental_strategy='merge',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro', 'tabela'],
        cluster_by=['data_registro'],
        description='Série temporal de ingestão de dados por data de envio do prontuário PCSM'
    )
}}

with
-- Aparentemente, a tabela de staging do PCSM só guarda dados do dia atual e não mantém histórico
staging as (
  select 
    seqatend,
    'atendimentos' as tabela,
    _airbyte_extracted_at as datalake_loaded_at,
    dtentrada as source_updated_at,
    datetime_diff(
          date(_airbyte_extracted_at, 'America/Sao_Paulo'),
          dtentrada, 
          second
    ) as diferenca_segundos
  from `rj-sms.brutos_prontuario_carioca_saude_mental_staging.gh_atendimentos`
),
ingestao as (
    select 
      tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct seqatend) as registros,
      min(diferenca_segundos) as menor_diferenca_segundos,
      avg(diferenca_segundos) as diferenca_media_segundos,
      approx_quantiles(diferenca_segundos, 100)[offset(50)] as diferenca_mediana_segundos,
      max(diferenca_segundos) as maior_diferenca_segundos
    from staging
    group by tabela, data_envio
),
final as (
    select 
        data_envio as data_registro,
        tabela,
        time(start_time, 'America/Sao_Paulo') as start_time,
        time(end_time, 'America/Sao_Paulo') as end_time,
        registros,
        round(menor_diferenca_segundos, 2) as min_diff,
        round(diferenca_media_segundos, 2) as media_diff,
        round(diferenca_mediana_segundos, 2) as mediana_diff,
        round(maior_diferenca_segundos, 2) as max_diff
    from ingestao
  )

select * from final