{{ config(
    alias="vacina",
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    schema="brutos_prontuario_vitacare_api",
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    }
) }}

{% set last_partition = get_last_partition_date(this) %}

with bruto_atendimento as (
    select
        source_id                                                         as id_prontuario_local,
        concat(nullif(payload_cnes, ''), '.', nullif(source_id, ''))      as id_prontuario_global,
        nullif(payload_cnes, '')                                          as id_cnes,
        safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim,
        date(safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime)) as data_particao,
        safe_cast(datalake_loaded_at as datetime)                         as loaded_at,
        data
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    {% if is_incremental() %}
      WHERE DATE(loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
    {% endif %}
    qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

exploded as (
    select
      id_prontuario_local,
      id_prontuario_global,
      id_cnes,
      data_particao,
      loaded_at,
      vac
    from bruto_atendimento,
    unnest(json_extract_array(data, '$.vacinas')) as vac
),

vacinas_dedup as (
    select * from exploded
),

fato_vacinas as (
    select
      *,
      row_number() over (
        partition by id_prontuario_global, json_extract_scalar(vac, '$.cod_vacina')
        order by loaded_at desc
      ) as rn
    from vacinas_dedup
    qualify rn = 1
)

select
  id_prontuario_global,
  id_prontuario_local,
  id_cnes,
  {{ process_null("json_extract_scalar(vac, '$.nome_vacina')") }}                          as nome_vacina,
  {{ process_null("json_extract_scalar(vac, '$.cod_vacina')") }}                           as cod_vacina,
  {{ process_null("json_extract_scalar(vac, '$.dose')") }}                                 as dose,
  {{ process_null("trim(json_extract_scalar(vac, '$.lote'))") }}                           as lote,
  safe_cast(
    substr(
      json_extract_scalar(vac, '$.datahora_aplicacao'),
      1,
      10
    ) as date
  )                                                                                         as data_aplicacao,
  timestamp_add(
    datetime(
      safe_cast(json_extract_scalar(vac, '$.datahora_registro') as timestamp),
      'America/Sao_Paulo'
    ),
    interval 3 hour
  )                                                                                         as data_registro,
  {{ process_null("json_extract_scalar(vac, '$.diff')") }}                                  as diff,
  {{ process_null("json_extract_scalar(vac, '$.calendario_vacinal_atualizado')") }}        as calendario_vacinal_atualizado,
  {{ process_null("json_extract_scalar(vac, '$.tipo_registro')") }}                        as tipo_registro,
  {{ process_null("json_extract_scalar(vac, '$.estrategia_imunizacao')") }}                as estrategia_imunizacao,
  CAST(NULL AS STRING)                                                                      as foi_aplicada,
  CAST(NULL AS STRING)                                                                      as justificativa,
  loaded_at,
  data_particao
from fato_vacinas