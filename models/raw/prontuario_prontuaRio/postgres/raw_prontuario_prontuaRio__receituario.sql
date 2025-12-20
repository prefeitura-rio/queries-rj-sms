{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="receituario",
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
  select * from {{source('brutos_prontuario_prontuaRio_staging', 'hp_rege_receituario') }}
  ),

  receituario as (
    select
      json_extract_scalar(data, '$.co_receituario') as id_receituario,
      json_extract_scalar(data, '$.prontuario') as id_prontuario,
      json_extract_scalar(data, '$.be') as id_boletim,
      json_extract_scalar(data, '$.co_internacao') as id_internacao,
      json_extract_scalar(data, '$.ds_receituario') as descricao, -- limpar html
      json_extract_scalar(data, '$.cpf_profissional') as profissional_cpf,
      json_extract_scalar(data, '$.no_profissional') as profissional_nome,
      json_extract_scalar(data, '$.dt_registro') as data_registro,
      json_extract_scalar(data, '$.ds_atividade') as descricao_atividade,
      cnes,
      loaded_at
from source_
),

final as (
    select 
        safe_cast(id_receituario as int64) as id_receituario,
        safe_cast(id_prontuario as int64) as id_prontuario,
        safe_cast(id_boletim as int64) as id_boletim,
        safe_cast(id_internacao as int64) as id_internacao,
        {{remove_html('descricao') }} as descricao,
        case 
            when profissional_cpf like "%000%"
                then cast(null as string)
            when profissional_cpf = '0'
                then cast(null as string)
            else {{ process_null('profissional_cpf') }} 
        end as profissional_cpf,
        {{ process_null('profissional_nome') }} as profissional_nome,
        safe_cast(data_registro as datetime) as data_registro,
        {{ process_null('descricao_atividade') }} as descricao_atividade,
        cnes,
        loaded_at,
        cast(safe_cast(loaded_at as timestamp) as date) as data_particao
    from receituario
    qualify row_number() over(partition by id_receituario, cnes order by loaded_at desc) = 1
)

select 
    concat(cnes, '.', id_receituario) as gid_receituario,
    concat(cnes, '.', id_prontuario) as gid_prontuario,
    concat(cnes, '.', id_boletim) as gid_boletim,
    concat(cnes, '.', id_internacao) as gid_internacao,
    *
from final