{{ config(
    alias="alergia",
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
      *,
      cast(source_id as string) as id_prontuario_local,
      cast(concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as string) as id_prontuario_global,
      safe_cast(datalake_loaded_at as datetime) as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim_atendimento
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    {% if is_incremental() %}
      WHERE DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
    {% endif %}
    qualify row_number() over (partition by id_prontuario_global order by loaded_at desc) = 1
  ),

  alergias_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      payload_cnes as id_cnes,
      json_extract_scalar(al, '$.descricao') as alergias_anamnese_descricao,
      loaded_at,
      date(datahora_fim_atendimento) as data_particao
    from bruto_atendimento,
    unnest(ifnull(json_extract_array(data, '$.alergias_anamnese'), [])) as al
  )

select * from alergias_flat
