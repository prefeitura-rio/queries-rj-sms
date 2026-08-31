{{
    config(
        alias='serie_temporal_ingestao_mv',
        materialized='incremental',
        incremental_strategy='merge',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro', 'tabela'],
        cluster_by=['data_registro'],
        description='Série temporal de ingestão de dados por data de envio do prontuário MV'
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 7 day)"
) %}

{% set tabelas = [
    {'nome': 'admissao', 'fonte': 'admissao_continuo'},
    {'nome': 'alta', 'fonte': 'alta_continuo'},
    {'nome': 'anamnese', 'fonte': 'anamnese_continuo'},
    {'nome': 'bam', 'fonte': 'bam_continuo'},
    {'nome': 'evolucao', 'fonte': 'evolucao_continuo'},
    {'nome': 'gestante', 'fonte': 'gestante_continuo'},
    {'nome': 'atendimento', 'fonte': 'paciente_continuo'},
    {'nome': 'parecer', 'fonte': 'parecer_continuo'},
    {'nome': 'profissional', 'fonte': 'profissional_continuo'}
] %}

with 

staging as (
  {% for item in tabelas %}
    select
      '{{ item.nome }}' as tabela,
      data,
      datalake_loaded_at,
      datetime_diff(
        datetime(datalake_loaded_at, 'America/Sao_Paulo'),
        safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
        second
      ) as diferenca_segundos
    from {{ source('brutos_prontuario_mv_api_staging', item.fonte) }}
    {% if is_incremental() %}
      where date(datalake_loaded_at, 'America/Sao_Paulo') >= {{ partitions_to_replace }}
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
      count(distinct data) as registros,
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