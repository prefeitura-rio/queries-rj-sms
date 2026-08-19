{{
    config(
        alias='serie_temporal_ingestao_unidade_vitacare',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de ingestão de dados por data de envio do prontuário Vitacare'
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 7 day)"
) %}

with 

staging as (
  select 
    'agendamento' as tabela,
    source_id,
    payload_cnes,
    datalake_loaded_at,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', source_updated_at) as source_updated_at,
    datetime_diff(
      datetime(datalake_loaded_at, 'America/Sao_Paulo'),
      safe.parse_datetime('%Y-%m-%d %H:%M:%S', source_updated_at), 
      second
    ) as diferenca_segundos
  from {{ source('brutos_prontuario_vitacare_api_staging', 'agendamento_continuo') }}
  {% if is_incremental() %}
    where date(datalake_loaded_at, 'America/Sao_Paulo') >= {{ partitions_to_replace }}
  {% endif %}
  union all

  select 
    'atendimento' as tabela,
    source_id,
    payload_cnes,
    datalake_loaded_at,
    safe.parse_datetime('%Y-%m-%d %H:%M:%S', source_updated_at) as source_updated_at,
    datetime_diff(
      datetime(datalake_loaded_at, 'America/Sao_Paulo'),
      safe.parse_datetime('%Y-%m-%d %H:%M:%S', source_updated_at), 
      second
    ) as diferenca_segundos
  from {{ source('brutos_prontuario_vitacare_api_staging', 'atendimento_continuo') }}
  {% if is_incremental() %}
    where date(datalake_loaded_at, 'America/Sao_Paulo') >= {{ partitions_to_replace }}
  {% endif %}
  union all

  select 
    'paciente' as tabela,
    patient_cpf as source_id,
    payload_cnes,
    datalake_loaded_at,
    safe.parse_datetime('%Y-%m-%dT%H:%M:%S', source_updated_at) as source_updated_at,
    datetime_diff(
      datetime(datalake_loaded_at, 'America/Sao_Paulo'),
      safe.parse_datetime('%Y-%m-%dT%H:%M:%S', source_updated_at), 
      second
    ) as diferenca_segundos
  from {{ source('brutos_prontuario_vitacare_api_staging', 'paciente_continuo') }}
    {% if is_incremental() %}
    where date(datalake_loaded_at, 'America/Sao_Paulo') >= {{ partitions_to_replace }}
  {% endif %}
),
ingestao as (
    select 
      tabela,
      payload_cnes,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct source_id) as registros,
      min(diferenca_segundos) as menor_diferenca_segundos,
      avg(diferenca_segundos) as diferenca_media_segundos,
      approx_quantiles(diferenca_segundos, 100)[offset(50)] as diferenca_mediana_segundos,
      max(diferenca_segundos) as maior_diferenca_segundos
    from staging
    group by tabela, data_envio, payload_cnes
),
estabelecimentos as (
  select 
    id_cnes, 
    nome_acentuado as nome
  from {{ref('dim_estabelecimento')}}
),

  final as (
    select 
        data_envio as data_registro,
        payload_cnes as cnes,
        {{ proper_estabelecimento('e.nome') }} as nome,
        tabela,
        time(start_time, 'America/Sao_Paulo') as start_time,
        time(end_time, 'America/Sao_Paulo') as end_time,
        registros,
        round(menor_diferenca_segundos, 2) as min_diff,
        round(diferenca_media_segundos, 2) as media_diff,
        round(diferenca_mediana_segundos, 2) as mediana_diff,
        round(maior_diferenca_segundos, 2) as max_diff
    from ingestao i
    left join estabelecimentos e on e.id_cnes = i.payload_cnes 
  )

select * from final