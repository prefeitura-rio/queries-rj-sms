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
      safe_cast(json_extract_scalar(data, '$.dt_registro') as datetime) as data_registro,
      json_extract_scalar(data, '$.ds_atividade') as descricao_atividade,
      cnes,
      loaded_at
from source_
),

final as (
    select 
        {{ process_null('id_receituario') }} as id_receituario,
        {{ process_null('id_prontuario') }} as id_prontuario,
        {{ process_null('id_boletim') }} as id_boletim,
        {{ process_null('id_internacao') }} as id_internacao,
        {{remove_html('descricao') }} as descricao,
        {{ process_null('profissional_cpf') }} as profissional_cpf,
        {{ process_null('profissional_nome') }} as profissional_nome,
        data_registro,
        {{ process_null('descricao_atividade') }} as descricao_atividade,
        cnes,
        loaded_at,
        cast(safe_cast(loaded_at as timestamp) as date) as data_particao
    from receituario
    qualify row_number() over(partition by id_receituario, id_prontuario, id_boletim, cnes order by loaded_at desc) = 1
)

select * from final