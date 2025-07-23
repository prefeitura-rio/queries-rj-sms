{{ config(
  alias="prescricao",
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
      source_id                                                     as id_prontuario_local,
      concat(nullif(payload_cnes, ''), '.', nullif(source_id, ''))  as id_prontuario_global,
      payload_cnes                                                  as id_cnes,
      safe_cast(datalake_loaded_at as datetime)                     as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim,
      data
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    qualify
      row_number() over(
        partition by id_prontuario_global
        order by datalake_loaded_at desc
      ) = 1
  ),

  prescricoes_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      id_cnes,
      {{ process_null("json_extract_scalar(pr, '$.nome_medicamento')") }} as medicamento_nome,
      {{ process_null("json_extract_scalar(pr, '$.cod_medicamento')") }}  as id_medicamento,
      {{ process_null("json_extract_scalar(pr, '$.posologia')") }}        as posologia,
      safe_cast(json_extract_scalar(pr, '$.quantidade') as numeric)       as quantidade,
      {{ process_null("json_extract_scalar(pr, '$.uso_continuado')") }}   as uso_continuado,
      loaded_at,
      date(datahora_fim)                                                  as data_particao
    from bruto_atendimento,
    unnest(json_extract_array(data, '$.prescricoes')) as pr
  ),

  prescricoes_dedup as (
    select
      *,
      row_number() over(
        partition by id_prontuario_global, id_medicamento
        order by loaded_at desc
      ) as rn
    from prescricoes_flat
  )

select
  id_prontuario_global,
  id_prontuario_local,
  id_cnes,
  medicamento_nome,
  id_medicamento,
  posologia,
  quantidade,
  uso_continuado,
  safe_cast(loaded_at as string) as loaded_at,
  data_particao
from prescricoes_dedup
where rn = 1

{% if is_incremental() %}
  and data_particao >= date_sub(current_date('America/Sao_Paulo'), interval 30 day)
{% endif %}
