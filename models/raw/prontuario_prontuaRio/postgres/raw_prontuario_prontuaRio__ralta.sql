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
        safe_cast(json_extract_scalar(data, '$.dt_registro') as datetime) as registro_data,
        json_extract_scalar(data, '$.ds_ralta') as descricao_alta,
        json_extract_scalar(data, '$.cpf_profissional') as profissional_cpf,
        json_extract_scalar(data, '$.no_profissional') as profissional_nome,
        json_extract_scalar(data, '$.ds_atividade') as descricao_atividade,
        cnes,
        loaded_at
    from source_
  ),

  deduplicated as (
    select * from ralta 
    qualify row_number() over(partition by id_alta, id_prontuario, id_boletim order by registro_data desc) = 1
  )

select *, cast(safe_cast(loaded_at as timestamp) as date) as data_particao
from deduplicated