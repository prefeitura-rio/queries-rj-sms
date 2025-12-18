{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="ralta",
        materialized="table",
        tags=["prontuaRio"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with 
  source_ as (
    select *
    from {{source('brutos_prontuario_prontuaRio_staging', 'hp_rege_ralta') }}
  ),

  ralta as (
    select
        json_extract_scalar(data, '$.co_ralta') as id_alta,
        json_extract_scalar(data, '$.prontuario') as id_prontuario,
        json_extract_scalar(data, '$.be') as id_boletim,
        json_extract_scalar(data, '$.co_internacao') as id_internacao,
        json_extract_scalar(data, '$.dt_registro') as registro_data,
        json_extract_scalar(data, '$.ds_ralta') as descricao_alta,
        json_extract_scalar(data, '$.cpf_profissional') as profissional_cpf,
        json_extract_scalar(data, '$.no_profissional') as profissional_nome,
        json_extract_scalar(data, '$.ds_atividade') as descricao_atividade,
        cnes,
        loaded_at
    from source_
  ),

  final as (
    select 
        safe_cast(id_alta as int64) as id_alta,
        safe_cast(id_prontuario as int64) as id_prontuario,
        safe_cast(id_boletim as int64) as id_boletim,
        safe_cast(id_internacao as int64) as id_internacao,
        safe_cast(registro_data as datetime) as registro_data,
        {{ remove_html('descricao_alta') }} as descricao_alta,
        case 
          when profissional_cpf like "%000%"
            then cast(null as string)
          when profissional_cpf = '0'
            then cast(null as string)
          else {{ process_null('profissional_cpf') }} 
        end as profissional_cpf,
        {{ process_null('profissional_nome') }} as profissional_nome,
        {{ process_null('descricao_atividade') }} as descricao_atividade,
        cnes,
        loaded_at,
        cast(safe_cast(loaded_at as timestamp) as date) as data_particao
    from ralta
    qualify row_number() over(partition by id_alta, id_prontuario, id_boletim, cnes order by loaded_at desc) = 1
  )

select
  concat(cnes, '.', id_alta) as gid_alta,
  concat(cnes, '.', id_prontuario) as gid_prontuario,
  concat(cnes, '.', id_boletim) as gid_boletim,
  concat(cnes, '.', id_internacao) as gid_internacao,
  *
from final