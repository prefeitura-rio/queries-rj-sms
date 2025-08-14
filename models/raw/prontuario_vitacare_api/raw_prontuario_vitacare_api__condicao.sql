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

{% set last_partition = get_last_partition_date(this) %}

with

  bruto_atendimento as (
    select
      safe_cast(source_id as string) as id_prontuario_local,
      concat(safe_cast(payload_cnes as string), '.', safe_cast(source_id as string)) as id_prontuario_global,  
      safe_cast(payload_cnes as string) as id_cnes,
      safe_cast(datalake_loaded_at as datetime) as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim_atendimento,
      data
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    {% if is_incremental() %}
      WHERE DATE(loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
    {% endif %}
    qualify row_number() over (partition by id_prontuario_global order by loaded_at desc) = 1
  ),

  condicoes_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      id_cnes,
      json_extract_scalar(cond, '$.cod_cid10') as cod_cid10,
      json_extract_scalar(cond, '$.estado') as estado,
      safe_cast(json_extract_scalar(cond, '$.data_diagnostico') as datetime) as data_diagnostico,
      loaded_at,
      date(coalesce(datahora_fim_atendimento, loaded_at)) as data_particao
    from bruto_atendimento,
    unnest(ifnull(json_extract_array(data, '$.condicoes'), [])) as cond
  ),

  condicoes_dedup as (
    select
      *,
      row_number() over (
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