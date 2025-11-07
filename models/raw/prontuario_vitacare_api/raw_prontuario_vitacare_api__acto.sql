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

{% set last_partition = get_last_partition_date(this) %}

with
  bruto_atendimento as (
    select
      cast(source_id as string) as id_prontuario_local,
      cast(concat(nullif(payload_cnes,''),'.',nullif(source_id,'')) as string) as id_prontuario_global,
      cast(payload_cnes as string) as id_cnes,
      cast(patient_cpf as string) as patient_cpf,
      safe_cast(datalake_loaded_at as datetime) as loaded_at,
      safe_cast(json_extract_scalar(data, '$.datahora_fim_atendimento') as datetime) as datahora_fim_atendimento,
      data
    from {{ source("brutos_prontuario_vitacare_api_staging", "atendimento_continuo") }}
    {% if is_incremental() %}
      WHERE DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
    {% endif %}
    qualify row_number() over (partition by id_prontuario_global order by loaded_at desc) = 1
  ),

  acto_flat as (
    select
      id_prontuario_global,
      id_prontuario_local,
      id_cnes,
      cast(json_extract_scalar(data,'$.unidade_ap') as string) as unidade_ap,
      patient_cpf,
      null as patient_code,
      cast(json_extract_scalar(data,'$.profissional.cns') as string) as profissional_cns,
      cast(json_extract_scalar(data,'$.profissional.cpf') as string) as profissional_cpf,
      cast(json_extract_scalar(data,'$.profissional.nome') as string) as profissional_nome,
      cast(json_extract_scalar(data,'$.profissional.cbo') as string) as profissional_cbo,
      cast(json_extract_scalar(data,'$.profissional.cbo_descricao') as string) as profissional_cbo_descricao,
      cast(json_extract_scalar(data,'$.profissional.equipe.nome') as string) as profissional_equipe_nome,
      cast(json_extract_scalar(data,'$.profissional.equipe.cod_ine') as string) as profissional_equipe_cod_ine,
      safe_cast(json_extract_scalar(data,'$.datahora_inicio_atendimento') as datetime) as datahora_inicio_atendimento,
      datahora_fim_atendimento,
      safe_cast(json_extract_scalar(data,'$.datahora_marcacao_atendimento') as datetime) as datahora_marcacao_atendimento,
      {{ process_null("json_extract_scalar(data,'$.tipo_consulta')") }} as tipo_consulta,
      safe_cast({{ process_null("json_extract_scalar(data,'$.eh_coleta')") }} as boolean) as eh_coleta,
      {{ process_null("json_extract_scalar(data,'$.soap_subjetivo_motivo')") }} as subjetivo_motivo,
      {{ process_null("json_extract_scalar(data,'$.soap_plano_observacoes')") }} as plano_observacoes,
      {{ process_null("json_extract_scalar(data,'$.soap_avaliacao_observacoes')") }} as avaliacao_observacoes,
      {{ process_null("json_extract_scalar(data,'$.soap_objetivo_descricao')") }} as objetivo_descricao,
      {{ process_null("json_extract_scalar(data,'$.notas_observacoes')") }} as notas_observacoes,
      {{ process_null("json_extract_scalar(data,'$.ut_id')") }} as ut_id,
      safe_cast({{ process_null("json_extract_scalar(data,'$.consulta_realizada')") }} as boolean) as realizado,
      {{ process_null("json_extract_scalar(data,'$.saude_bucal[0].tipo_atendimento')") }} as tipo_atendimento,
      loaded_at,
      date(datahora_fim_atendimento) as data_particao
    from bruto_atendimento
  )

select * from acto_flat