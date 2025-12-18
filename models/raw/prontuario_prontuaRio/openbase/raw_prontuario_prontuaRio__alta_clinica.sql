{{
    config(
        alias="alta_clinica",
        materialized="table",
        unique_key="id_prontuario",
        tags=["prontuaRio"],
        schema="brutos_prontuario_prontuaRio",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        }
    )
}}

with 
  source_ as (
    select *
    from {{ source('brutos_prontuario_prontuaRio_staging', 'alta_clinica') }} 
),

  alta_clinica as (
    select 
      json_extract_scalar(data,'$.a_pront') as id_prontuario,
      
      case
        when json_extract_scalar(data,'$.a_dinter') = '00000000' 
          then null
        when regexp_contains(json_extract_scalar(data,'$.a_dinter'), r'^\d{8}$') 
          then parse_date('%Y%m%d' ,json_extract_scalar(data,'$.a_dinter'))
        else null
      end as internacao_data,

      case 
        when regexp_contains(json_extract_scalar(data,'$.a_hinter'), r'^\d{4}$') 
          then parse_time('%H%M',json_extract_scalar(data,'$.a_hinter'))
        else null
      end as internacao_hora,

      case 
        when json_extract_scalar(data,'$.a_dalta') = '00000000' 
          then null
        when regexp_contains(json_extract_scalar(data,'$.a_dalta'), r'^\d{8}$') 
          then parse_date('%Y%m%d' ,json_extract_scalar(data,'$.a_dalta'))
        else null
      end as alta_data,

      case 
        when regexp_contains(json_extract_scalar(data,'$.a_halta'), r'^\d{4}$') 
          then parse_time('%H%M',json_extract_scalar(data,'$.a_halta'))
        else null
      end as alta_hora,

      json_extract_scalar(data,'$.a_motivo_saida') as saida_saida,
      json_extract_scalar(data,'$.a_motivo_alta') as alta_motivo,
      json_extract_scalar(data,'$.a_codclin') as id_clinica,
      json_extract_scalar(data,'$.a_codleito') as id_leito,
      json_extract_scalar(data,'$.a_codunidade') as id_unidade,
      json_extract_scalar(data,'$.a_cpfalta') as alta_cpf,
      json_extract_scalar(data,'$.a_proc') as proc,
      json_extract_scalar(data,'$.a_cid10') as codigo_cid10,
      json_extract_scalar(data,'$.a_status') as status,
      cnes,
      loaded_at
    from source_
),

final as (
  select 
      safe_cast(id_prontuario as int64) as id_prontuario,
      internacao_data,
      internacao_hora,
      alta_data,
      alta_hora,
      {{ process_null('saida_saida') }} as saida_saida,
      {{ process_null('alta_motivo') }} as alta_motivo,
      {{ process_null('id_clinica') }} as id_clinica,
      {{ process_null('id_leito') }} as id_leito,
      {{ process_null('id_unidade') }} as id_unidade,
      case 
        when alta_cpf like '%000%'
          then cast(null as string)
        else {{ process_null('alta_cpf') }}
      end as alta_cpf,
      {{ process_null('proc') }} as proc,
      {{ process_null('codigo_cid10') }} as codigo_cid10,
      {{ process_null('status') }} as status,
      cnes,
      loaded_at,
      cast(safe_cast(loaded_at as timestamp) as date) as data_particao
    from alta_clinica
)

select 
  concat(cnes, '.', id_prontuario) as gid_prontuario,
  concat(cnes, '.', id_clinica) as gid_clinica,
  concat(cnes, '.', id_leito) as gid_leito,
  concat(cnes, '.', id_unidade) as gid_unidade,
  *
from final