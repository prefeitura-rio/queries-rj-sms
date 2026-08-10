{{
    config(
        alias='serie_temporal_ingestao_mv',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "month"
        },
        unique_key=['data_registro'],
        description='Série temporal de ingestão de dados por data de envio do prontuário MV'
    )
}}

with 
  ingestao as (
    select 
      'admissao' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
      min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
    from rj-sms.brutos_prontuario_mv_api_staging.admissao_continuo
    group by data_envio

    union all

    select 
      'alta' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
            min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
      
    from rj-sms.brutos_prontuario_mv_api_staging.alta_continuo
    group by data_envio

    union all

    select 
      'anamnese' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
            min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
    from rj-sms.brutos_prontuario_mv_api_staging.anamnese_continuo
    group by data_envio

    union all

    select 
      'bam' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
            min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
    from rj-sms.brutos_prontuario_mv_api_staging.bam_continuo
    group by data_envio

    union all

    select 
      'evolucao' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
            min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
    from rj-sms.brutos_prontuario_mv_api_staging.evolucao_continuo
    group by data_envio

    union all

    select 
      'gestante' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
            min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
    from rj-sms.brutos_prontuario_mv_api_staging.gestante_continuo
    group by data_envio

    union all

    select 
      'atendimento' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
            min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
    from rj-sms.brutos_prontuario_mv_api_staging.paciente_continuo
    group by data_envio

    union all

    select 
      'parecer' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
            min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
    from rj-sms.brutos_prontuario_mv_api_staging.parecer_continuo
    group by data_envio

    union all

    select 
      'profissional' as tabela,
      date(datalake_loaded_at, 'America/Sao_Paulo') as data_envio,
      min(datalake_loaded_at) as start_time,
      max(datalake_loaded_at) as end_time,
      count(distinct data) as registros,
            min(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) menor_diferenca_segundos,
      avg(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) as diferenca_media_segundos,
      max(
        datetime_diff(
          datetime(datalake_loaded_at, 'America/Sao_Paulo'),
          safe.parse_datetime('%Y/%m/%d %H:%M:%S', source_updated_at), 
          second
        )
      ) maior_diferenca_segundos
    from rj-sms.brutos_prontuario_mv_api_staging.profissional_continuo
    group by data_envio
  ),

  final as (
    select 
        data_envio,
        tabela,
        time(start_time, 'America/Sao_Paulo') as start_time,
        time(end_time, 'America/Sao_Paulo') as end_time,
        registros,
        round(menor_diferenca_segundos, 2) as min_diff,
        round(diferenca_media_segundos, 2) as media_diff,
        round(maior_diferenca_segundos, 2) as max_diff
    from ingestao
  )


select * from final

