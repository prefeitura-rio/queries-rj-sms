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

with

  bruto_atualizado as (
    select
      *,
      source_id  as id_prontuario_local,
      concat(nullif(payload_cnes, ''), '.', nullif(source_id, '')) as id_prontuario_global,
      safe_cast(datalake_loaded_at as datetime)               as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime)
                                                              as datahora_fim
    from {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
    qualify
      row_number()
      over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
  ),

  alergias_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      payload_cnes        as id_cnes,
      json_extract_scalar(al, '$.descricao') as alergias_anamnese_descricao,
      loaded_at,
      date(datahora_fim)  as data_particao
    from bruto_atualizado,
    unnest(json_extract_array(data, '$.alergias_anamnese')) as al
  )

select * from alergias_flat

{% if is_incremental() %}
  where data_particao >= date_sub(current_date('America/Sao_Paulo'), interval 30 day)
{% endif %}
