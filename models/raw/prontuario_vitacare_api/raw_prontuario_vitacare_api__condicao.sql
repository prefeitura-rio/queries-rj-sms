

{{ config(
  alias="condicao",
  schema="brutos_prontuario_vitacare_api",
  materialized="incremental",
  incremental_strategy="insert_overwrite",
  partition_by={
    "field": "data_particao",
    "data_type": "date",
    "granularity": "day"
  }
) }}

with

  bruto_atendimento as (
    select
      safe_cast(source_id as string) as id_prontuario_local,
      concat(safe_cast(payload_cnes as string), '.', safe_cast(source_id as string)) as id_prontuario_global,  
      safe_cast(payload_cnes as string) as id_cnes,
      safe_cast(datalake_loaded_at as datetime)                      as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim,
      data
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    {% if is_incremental() %}
      where date(safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime)) 
            >= date_sub(current_date('America/Sao_Paulo'), interval 30 day)
    {% endif %}
    qualify
      row_number() over(
        partition by id_prontuario_global 
        order by datalake_loaded_at desc
      ) = 1
  ),

  condicoes_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      id_cnes,
      json_extract_scalar(cond, '$.cod_cid10')  as cod_cid10,
      json_extract_scalar(cond, '$.estado')     as estado,
      safe_cast(json_extract_scalar(cond, '$.data_diagnostico') as datetime) as data_diagnostico,
      loaded_at,
      date(datahora_fim) as data_particao
    from bruto_atendimento,
    unnest(json_extract_array(data, '$.condicoes')) as cond
  ),

  condicoes_dedup as (
    select
      *,
      row_number() over(
        partition by id_prontuario_global, cod_cid10
        order by loaded_at desc
      ) as rn
    from condicoes_flat
  )

select
  id_prontuario_global,
  id_prontuario_local,
  id_cnes,
  cod_cid10,
  estado,
  data_diagnostico,
  loaded_at,
  data_particao
from condicoes_dedup
where rn = 1