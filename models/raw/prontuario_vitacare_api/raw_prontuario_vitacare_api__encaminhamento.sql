{{ config(
  alias="encaminhamento",
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
      source_id as id_prontuario_local,
      concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as id_prontuario_global,
      payload_cnes as id_cnes,
      safe_cast(datalake_loaded_at as datetime) as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim,
      data
      from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
      {% if is_incremental() %}
        where date(safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime))
              >= date_sub(current_date('America/Sao_Paulo'), interval 30 day)
      {% endif %}
      qualify
        row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
  ),

  encaminhamentos_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      id_cnes,
      json_extract_scalar(enc, '$.descricao') as encaminhamento_especialidade,
      loaded_at,
      date(datahora_fim) as data_particao
    from bruto_atendimento,
    unnest(json_extract_array(data, '$.encaminhamentos')) as enc
  ),

  encaminhamentos_dedup as (
    select
      *,
      row_number() over(
        partition by id_prontuario_global, encaminhamento_especialidade
        order by loaded_at desc
      ) as rn
    from encaminhamentos_flat
  )

select
  id_prontuario_global,
  id_prontuario_local,
  id_cnes,
  encaminhamento_especialidade,
  safe_cast(loaded_at as string) as loaded_at,
  data_particao
from encaminhamentos_dedup
where rn = 1
