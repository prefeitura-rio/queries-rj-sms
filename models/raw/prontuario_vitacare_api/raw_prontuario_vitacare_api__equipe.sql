{{ config(
  alias="equipe",
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
      cast(payload_cnes as string) as id_cnes,
      json_extract_scalar(data, '$.profissional.equipe.cod_equipe') as codigo,
      json_extract_scalar(data, '$.profissional.equipe.cod_ine') as n_ine,
      json_extract_scalar(data, '$.profissional.equipe.nome') as nome,
      safe_cast(datalake_loaded_at as datetime) as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim_atendimento
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    WHERE JSON_EXTRACT(data, '$.profissional.equipe') IS NOT NULL
    AND JSON_EXTRACT(data, '$.profissional.equipe') != '[]'
    {% if is_incremental() %}
      AND DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
    {% endif %}
    qualify row_number() over(partition by codigo order by loaded_at desc) = 1
  ),

  equipe_flat as (
    select
      id_cnes,
      codigo,
      n_ine,
      nome,
      loaded_at,
      date(coalesce(datahora_fim_atendimento, loaded_at)) as data_particao
    from bruto_atendimento
  )

select
  id_cnes,
  null as id,
  codigo,
  nome,
  n_ine,
  loaded_at,
  data_particao
from equipe_flat