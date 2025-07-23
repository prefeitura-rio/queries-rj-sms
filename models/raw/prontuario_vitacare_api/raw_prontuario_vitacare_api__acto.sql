{{ config(
  alias="acto",
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
      concat(nullif(payload_cnes,''),'.',nullif(source_id,'')) as id_prontuario_global,
      payload_cnes as id_cnes,
      patient_cpf,
      safe_cast(datalake_loaded_at as datetime) as loaded_at,
      data
    from {{ source("brutos_prontuario_vitacare_staging","atendimento_continuo") }}
    qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc)=1
  ),

  acto_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      id_cnes,
      json_extract_scalar(data,'$.unidade_ap') as unidade_ap,
      patient_cpf,
      null as patient_code,
      json_extract_scalar(data,'$.profissional.cns') as profissional_cns,
      json_extract_scalar(data,'$.profissional.cpf') as profissional_cpf,
      json_extract_scalar(data,'$.profissional.nome') as profissional_nome,
      json_extract_scalar(data,'$.profissional.cbo') as profissional_cbo,
      json_extract_scalar(data,'$.profissional.cbo_descricao') as profissional_cbo_descricao,
      json_extract_scalar(data,'$.profissional.equipe.nome') as profissional_equipe_nome,
      json_extract_scalar(data,'$.profissional.equipe.cod_ine') as profissional_equipe_cod_ine,
      safe_cast(json_extract_scalar(data,'$.datahora_inicio_atendimento') as datetime) as datahora_inicio_atendimento,
      safe_cast(json_extract_scalar(data,'$.datahora_fim_atendimento') as datetime) as datahora_fim_atendimento,
      safe_cast(json_extract_scalar(data,'$.datahora_marcacao_atendimento') as datetime) as datahora_marcacao_atendimento,
      {{ process_null("json_extract_scalar(data,'$.tipo_consulta')") }} as tipo_consulta,
      {{ process_null("json_extract_scalar(data,'$.eh_coleta')") }} as eh_coleta,
      {{ process_null("json_extract_scalar(data,'$.soap_subjetivo_motivo')") }} as subjetivo_motivo,
      {{ process_null("json_extract_scalar(data,'$.soap_plano_observacoes')") }} as plano_observacoes,
      {{ process_null("json_extract_scalar(data,'$.soap_avaliacao_observacoes')") }} as avaliacao_observacoes,
      {{ process_null("json_extract_scalar(data,'$.soap_objetivo_descricao')") }} as objetivo_descricao,
      {{ process_null("json_extract_scalar(data,'$.notas_observacoes')") }} as notas_observacoes,
      safe_cast({{ process_null("json_extract_scalar(data,'$.ut_id')") }} as INT64) as ut_id,
      safe_cast({{ process_null("json_extract_scalar(data,'$.realizado')") }} as BOOLEAN) as realizado,
      {{ process_null("json_extract_scalar(data,'$.tipo_atendimento')") }} as tipo_atendimento,
      loaded_at,
      date(safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime)) as data_particao
    from bruto_atendimento
  )

select * from acto_flat

{% if is_incremental() %}
where data_particao >= date_sub(current_date('America/Sao_Paulo'), interval 30 day)
{% endif %}