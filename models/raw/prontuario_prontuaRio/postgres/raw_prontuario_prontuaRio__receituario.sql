{{
    config(
        alias="receituario",
        materialized="table",
        tags=["prontuaRio"],
    )
}}

with 
  source_ as (
  select * from {{source('brutos_prontuario_prontuaRIO', 'hp_rege_receituario') }}
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
        {{ process_null('id_receituario') }},
        {{ process_null('id_prontuario') }},
        {{ process_null('id_boletim') }},
        {{ process_null('id_internacao') }},
        {{remove_html('descricao') }} as descricao,
        {{ process_null('profissional_cpf') }},
        {{ process_null('profissional_nome') }},
        {{ process_null('data_registro') }},
        {{ process_null('descricao_atividade') }},
        cnes,
        loaded_at,
        cast(loaded_at as date) as data_particao
    from receituario
    qualify row_number() over(partition by id_receituario, id_prontuario, id_boletim, cnes order by loaded_at desc) = 1
)

select * from final