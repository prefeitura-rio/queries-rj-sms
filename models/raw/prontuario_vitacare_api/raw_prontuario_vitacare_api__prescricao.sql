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

{% set last_partition = get_last_partition_date(this) %}

with

  bruto_atendimento as (
    select
      source_id                                                     as id_prontuario_local,
      concat(nullif(payload_cnes, ''), '.', nullif(source_id, ''))  as id_prontuario_global,
      payload_cnes                                                  as id_cnes,
      safe_cast(datalake_loaded_at as datetime)                     as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim_atendimento,
      data
    from {{ source("brutos_prontuario_vitacare_api_staging", "atendimento_continuo") }}
    WHERE JSON_EXTRACT(data, '$.prescricoes') IS NOT NULL
    AND JSON_EXTRACT(data, '$.prescricoes') != '[]'
    {% if is_incremental() %}
      AND DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
    {% endif %}
    qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
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
      date(datahora_fim_atendimento)                                                  as data_particao
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
  loaded_at,
  data_particao
from prescricoes_dedup
where rn = 1
