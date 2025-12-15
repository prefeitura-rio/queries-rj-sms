{{
    config(
        schema='brutos_prontuario_prontuaRio',
        alias="atendimento",
        materialized="table",
        unique_key="id_prontuario",
        tags=["prontuaRio"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
 
  source_ as (
    select * 
    from {{source('brutos_prontuario_prontuaRio_staging', 'atendimento') }}
  ),

  atendimentos as (
    select 
      json_extract_scalar(data,'$.id_atendimento') as id_atendimento,
      json_extract_scalar(data,'$.id_paciente') as id_paciente,
      json_extract_scalar(data,'$.crm') as medico_crm,
      json_extract_scalar(data,'$.leito') as id_leito,
      json_extract_scalar(data,'$.clinica') as clinica,
      json_extract_scalar(data,'$.recomendacoes') as recomendacoes,
      safe_cast(json_extract_scalar(data,'$.data_hora') as datetime) as datahora,
      safe_cast(json_extract_scalar(data,'$.data_internacao') as date) as internacao_data,
      safe_cast(json_extract_scalar(data,'$.data_atendimento')as date) as atendimento_data,
      json_extract_scalar(data,'$.ativo') as ativo_indicador,
      json_extract_scalar(data,'$.cod_clin') as id_clinica,
      json_extract_scalar(data,'$.enfermeria') as enfermaria,
      json_extract_scalar(data,'$.parenteral') as parenteral,
      json_extract_scalar(data,'$.status_parenteral') as parenteral_status,
      json_extract_scalar(data,'$.peso') as peso,
      json_extract_scalar(data,'$.paren_neo') as paren_neo,
      json_extract_scalar(data,'$.paren_venosa') as paren_venosa,
      json_extract_scalar(data,'$.status_paren_neo') as paren_neo_status,
      json_extract_scalar(data,'$.status_parenvenosa') as paren_venosa_status,
      json_extract_scalar(data,'$.precisa_fono') as precisa_fono,
      json_extract_scalar(data,'$.precisa_fisio') as precisa_fisio,
      json_extract_scalar(data,'$.obs_fisio') as obs_fisio,
      json_extract_scalar(data,'$.obs_fono') as obs_fono,
      json_extract_scalar(data,'$.fisio_motora') as fisio_motora,
      json_extract_scalar(data,'$.fisio_respiratoria') as fisio_respiratoria,
      json_extract_scalar(data,'$.fisio_ok_motora') as fisio_ok_motora,
      json_extract_scalar(data,'$.avisos_farm') as avisos_farm,
      json_extract_scalar(data,'$.estatura') as estatura,
      json_extract_scalar(data,'$.sc') as sc,
      json_extract_scalar(data,'$.tht') as tht,
      json_extract_scalar(data,'$.med') as med,
      json_extract_scalar(data,'$.hv') as hv,
      json_extract_scalar(data,'$.dieta') as dieta,
      json_extract_scalar(data,'$.vm') as vm,
      json_extract_scalar(data,'$.paren_sedacao') as paren_sedacao,
      json_extract_scalar(data,'$.diag') as diag,
      json_extract_scalar(data,'$.ig') as ig,
      json_extract_scalar(data,'$.igc') as igc,
      json_extract_scalar(data,'$.precisa_psicologia') as precisa_psicologia,
      json_extract_scalar(data,'$.precisa_nutricao') as precisa_nutricao, 
      json_extract_scalar(data,'$.obs_psicologia') as obs_psicologia,
      json_extract_scalar(data,'$.obs_nutricao') as obs_nutricao,
      json_extract_scalar(data,'$.psicologia_ok') as psicologia_ok,
      json_extract_scalar(data,'$.sala') as sala,
      json_extract_scalar(data,'$.data_prescricao') as prescricao_data,
      json_extract_scalar(data,'$.precisa_hemo') as precisa_hemo, 
      json_extract_scalar(data,'$.obs_hemo') as obs_hemo,
      json_extract_scalar(data,'$.hemo_ok') as hemo_ok,
      cnes,
      loaded_at
    from source_
),

final as (
  select 
    {{ process_null('id_atendimento') }} as id_atendimento,
    {{ process_null('id_paciente') }} as id_paciente,
    {{ process_null('medico_crm') }} as medico_crm,
    {{ process_null('id_leito') }} as id_leito,
    {{ process_null('clinica') }} as clinica,
    {{ process_null('recomendacoes') }} as recomendacoes,
    datahora,
    internacao_data,
    atendimento_data,
    {{ process_null('ativo_indicador') }} as ativo_indicador,
    {{ process_null('id_clinica') }} as id_clinica,
    {{ process_null('enfermaria') }} as enfermaria,
    {{ process_null('parenteral') }} as parenteral,
    {{ process_null('parenteral_status') }} as parenteral_status,
    {{ process_null('peso') }} as peso,
    {{ process_null('paren_neo') }} as paren_neo,
    {{ process_null('paren_venosa') }} as paren_venosa,
    {{ process_null('paren_neo_status') }} as paren_neo_status,
    {{ process_null('paren_venosa_status') }} as paren_venosa_status,
    {{ process_null('precisa_fono') }} as precisa_fono,
    {{ process_null('precisa_fisio') }} as precisa_fisio,
    {{ process_null('obs_fisio') }} as obs_fisio,
    {{ process_null('obs_fono') }} as obs_fono,
    {{ process_null('fisio_motora') }} as fisio_motora,
    {{ process_null('fisio_respiratoria') }} as fisio_respiratoria,
    {{ process_null('fisio_ok_motora') }} as fisio_ok_motora,
    {{ process_null('avisos_farm') }} as avisos_farm,
    {{ process_null('estatura') }} as estatura,
    {{ process_null('sc') }} as sc,
    {{ process_null('tht') }} as tht,
    {{ process_null('med') }} as med,
    {{ process_null('hv') }} as hv,
    {{ process_null('dieta') }} as dieta,
    {{ process_null('vm') }} as vm,
    {{ process_null('paren_sedacao') }} as paren_sedacao,
    {{ process_null('diag') }} as diag,
    {{ process_null('ig') }} as ig,
    {{ process_null('igc') }} as igc,
    {{ process_null('precisa_psicologia') }} as precisa_psicologia,
    {{ process_null('precisa_nutricao') }} as precisa_nutricao, 
    {{ process_null('obs_psicologia') }} as obs_psicologia,
    {{ process_null('obs_nutricao') }} as obs_nutricao,
    {{ process_null('psicologia_ok') }} as psicologia_ok,
    {{ process_null('sala') }} as sala,
    prescricao_data,
    {{ process_null('precisa_hemo') }} as precisa_hemo, 
    {{ process_null('obs_hemo') }} as obs_hemo,
    {{ process_null('hemo_ok') }} as hemo_ok,
    cnes,
    loaded_at,
    cast(safe_cast(loaded_at as timestamp) as date) as data_particao
  from atendimentos
)

select 
    {{
        dbt_utils.generate_surrogate_key(
                [
                    'id_atendimento',
                    'id_paciente',
                    'cnes'
                ]
            )
        }} as id_hci,
    *
  from final