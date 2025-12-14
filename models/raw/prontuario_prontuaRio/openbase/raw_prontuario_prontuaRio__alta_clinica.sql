{{
    config(
        alias="alta_clinica",
        materialized="table",
        unique_key="id_prontuario",
        tags=["prontuaRio"],
    )
}}


with 
  source_ as (
    select *
    from {{ source('brutos_prontuario_prontuaRIO', 'alta_clinica') }} 
),

  alta_clinica as (
    select 
      json_extract_scalar(data,'$.a_pront') as id_prontuario,
      parse_date('%Y%m%d' ,json_extract_scalar(data,'$.a_dinter')) as internacao_data,
      parse_time("%H%M",json_extract_scalar(data,'$.a_hinter')) as internacao_hora,
      parse_date('%Y%m%d' ,json_extract_scalar(data,'$.a_dalta')) as alta_data,
      parse_time("%H%M",json_extract_scalar(data,'$.a_hinter')) as alta_hora,
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
)

deduplicated as (
  select * from alta_clinica 
  qualify row_number() over(partition by id_prontuario, cnes order by loaded_at desc) = 1
)
select 
    *,
    date(loaded_at) as data_particao 
from deduplicated