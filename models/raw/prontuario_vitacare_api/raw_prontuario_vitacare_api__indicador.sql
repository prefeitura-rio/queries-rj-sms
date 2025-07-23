{{ config(
  alias="indicador",
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

  bruto as (
    select
      source_id                                                      as id_prontuario_local,
      concat(nullif(payload_cnes, ''), '.', nullif(source_id, ''))  as id_prontuario_global,
      payload_cnes                                                   as id_cnes,
      safe_cast(datalake_loaded_at as datetime)                      as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim,
      data
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    qualify
      row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
  ),

  indicadores_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      id_cnes,
      json_extract_scalar(ind, '$.nome')        as indicadores_nome,
      json_extract_scalar(ind, '$.valor')       as valor,
      loaded_at,
      date(datahora_fim)                        as data_particao,
    from bruto,
    unnest(json_extract_array(data, '$.indicadores')) as ind
  ),

  indicadores_dedup as (
    select
      *,
      row_number() over (
        partition by id_prontuario_global, indicadores_nome
        order by loaded_at desc
      ) as rn
    from indicadores_flat
  )

select
  id_prontuario_global,
  id_prontuario_local,
  id_cnes,
  indicadores_nome,
  valor,
  safe_cast(loaded_at as string) as loaded_at,
  data_particao,
from indicadores_dedup
where rn = 1

{% if is_incremental() %}
  and data_particao >= date_sub(current_date('America/Sao_Paulo'), interval 30 day)
{% endif %}
