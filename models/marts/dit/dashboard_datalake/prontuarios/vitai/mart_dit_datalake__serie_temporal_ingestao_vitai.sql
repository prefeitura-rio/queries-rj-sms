{{
    config(
        alias='serie_temporal_ingestao_vitai',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de ingestão de dados por data de envio do prontuário Vitai'
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 7 day)"
) %}

{% set tabelas = [
    {'nome': 'boletim', 'tabela_origem': 'basecentral__boletim_eventos'},
    {'nome': 'alta', 'tabela_origem': 'basecentral__alta_eventos'},
    {'nome': 'atendimento', 'tabela_origem': 'basecentral__atendimento_eventos'},
    {'nome': 'diagnostico', 'tabela_origem': 'basecentral__diagnostico_eventos'},
    {'nome': 'internacao', 'tabela_origem': 'basecentral__internacao_eventos'},
    {'nome': 'paciente', 'tabela_origem': 'basecentral__paciente_eventos'}
] %}

with 

staging as (
  {% for item in tabelas %}
    select 
      gid,
      '{{ item.nome }}' as tabela,
      parse_timestamp('%Y-%m-%d %H:%M:%E*S%Ez', datalake_loaded_at) as datalake_loaded_at,
      cast(created_at as datetime) as data_registro,
      datetime_diff(
        datetime(parse_timestamp('%Y-%m-%d %H:%M:%E*S%Ez', datalake_loaded_at), 'America/Sao_Paulo'),
        cast(created_at as datetime), 
        second
      ) as diferenca_segundos
    from {{ source('brutos_prontuario_vitai_staging', item.tabela_origem) }}
    {% if is_incremental() %}
      where date(parse_timestamp('%Y-%m-%d %H:%M:%E*S%Ez', datalake_loaded_at), 'America/Sao_Paulo') >= {{ partitions_to_replace }}
    {% endif %}

    {% if not loop.last %} union all {% endif %}
  {% endfor %}
),

ingestao as (
    select 
      tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct gid) as registros,
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