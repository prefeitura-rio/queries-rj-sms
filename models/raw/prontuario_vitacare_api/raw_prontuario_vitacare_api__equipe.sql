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

with

  bruto as (
    select
      payload_cnes                                                       as id_cnes,
      json_extract_scalar(data, '$.profissional.equipe.cod_equipe')      as codigo,
      json_extract_scalar(data, '$.profissional.equipe.cod_ine')         as n_ine,
      json_extract_scalar(data, '$.profissional.equipe.nome')            as nome,
      safe_cast(datalake_loaded_at as datetime)                          as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    qualify row_number() over(partition by codigo order by datalake_loaded_at desc) = 1
  ),

  flat as (
    select
      id_cnes,
      codigo,
      n_ine,
      nome,
      loaded_at,
      date(datahora_fim)                                                as data_particao
    from bruto
  )

select
  id_cnes,
  null                                                             as id,
  codigo,
  nome,
  n_ine,
  safe_cast(loaded_at as string)                                     as loaded_at,
  data_particao
from flat

{% if is_incremental() %}
  where data_particao >= date_sub(current_date('America/Sao_Paulo'), interval 30 day)
{% endif %}
