{{
  config(
    alias="agendamento", 
    schema="brutos_prontuario_vitacare_api",
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    cluster_by= ['id_cnes', 'tipo_atendimento'],
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day"
    },
  )
}}

{% set last_partition = get_last_partition_date(this) %}

with  
  source_agendamento as (
    select 
      patient_cpf,
      source_updated_at,
      source_id,
      data,
      payload_cnes,
      datalake_loaded_at
      from {{ source("brutos_prontuario_vitacare_api_staging", "agendamento_continuo") }}
      {% if is_incremental() %}
        where datalake_loaded_at >= timestamp('{{ last_partition }}')
      {% endif %}
      qualify row_number() over (partition by source_id, payload_cnes order by datalake_loaded_at desc) = 1
  ),

  cast_agendamento as (
    select 
      cast(patient_cpf as string) as patient_cpf,
      cast(source_updated_at as datetime) as source_updated_at,
      cast(concat(nullif(payload_cnes,''),'.',nullif(source_id,'')) as string) as id_agendamento,
      
      cast(json_extract_scalar(data,'$.datahora_agendamento') as datetime) as datahora_agendamento,
      cast(json_extract_scalar(data,'$.datahora_marcacao_atendimento') as datetime) as datahora_marcacao_atendimento,
      cast(json_extract_scalar(data,'$.profissional.cns') as string) as profissional_cns,
      cast(json_extract_scalar(data,'$.profissional.cpf') as string) as profissional_cpf,
      cast(json_extract_scalar(data,'$.profissional.nome') as string) as profissional_nome,
      cast(json_extract_scalar(data,'$.profissional.cbo') as string) as profissional_cbo,
      cast(json_extract_scalar(data,'$.profissional.cbo_descricao') as string) as profissional_cbo_descricao,
      cast(json_extract_scalar(data,'$.profissional.equipe.nome') as string) as profissional_equipe_nome,
      cast(json_extract_scalar(data,'$.profissional.equipe.cod_equipe') as string) as profissional_equipe_codigo,
      cast(json_extract_scalar(data,'$.profissional.equipe.cod_ine') as string) as profissional_equipe_ine,
      cast(json_extract_scalar(data,'$.motivo_cancelamento') as string) as motivo_cancelamento,
      cast(json_extract_scalar(data,'$.consulta_realizada') as string) as consulta_realizada,
      cast(json_extract_scalar(data,'$.tipo_atendimento') as string) as tipo_atendimento,
      cast(json_extract_scalar(data,'$.estado_marcacao') as string) as estado_marcacao,
      cast(json_extract_scalar(data,'$.unidade_cnes') as string) as id_cnes,
      cast(json_extract_scalar(data,'$.ut_id') as string) as ut_id,

      cast(datalake_loaded_at as date) as data_particao
    from source_agendamento
  ),

  final as (
    select * from cast_agendamento
  )

select * from final

