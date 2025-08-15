{{ config(
  alias="solicitacao_exame",
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
      source_id                                                        as id_prontuario_local,
      concat(
        nullif(payload_cnes, ''), 
        '.', 
        nullif(source_id, '')
      )                                                                 as id_prontuario_global,
      {{ process_null("payload_cnes") }}                                as id_cnes,
      safe_cast(datalake_loaded_at as datetime)                        as loaded_at,
      safe_cast(
        json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime
      )                                                                 as datahora_fim,
      data
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
  {% if is_incremental() %}
    WHERE DATE(loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

  exames_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      id_cnes,
      {{ process_null("json_extract_scalar(ex, '$.nome_exame')") }}       as nome_exame,
      {{ process_null("json_extract_scalar(ex, '$.cod_exame')") }}        as cod_exame,
      safe_cast(
        {{ process_null("json_extract_scalar(ex, '$.quantidade')") }} 
        as numeric
      )                                                                 as quantidade,
      {{ process_null("json_extract_scalar(ex, '$.material')") }}         as material,
      safe_cast(
        {{ process_null("json_extract_scalar(ex, '$.data_solicitacao')") }} 
        as datetime
      )                                                                 as data_solicitacao,
      loaded_at,
      date(datahora_fim)                                               as data_particao
    from bruto_atendimento,
    unnest(json_extract_array(data, '$.exames_solicitados')) as ex
  ),

  exames_dedup as (
    select
      *,
      row_number() over(
        partition by id_prontuario_global, cod_exame
        order by loaded_at desc
      ) as rn
    from exames_flat
  )

select
  id_prontuario_global,
  id_prontuario_local,
  id_cnes,
  nome_exame,
  cod_exame,
  quantidade,
  material,
  data_solicitacao,
  loaded_at,
  data_particao
from exames_dedup
where rn = 1