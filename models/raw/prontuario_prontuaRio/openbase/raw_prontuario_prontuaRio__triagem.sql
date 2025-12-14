{{
    config(
        alias="triagem",
        materialized="table",
        tags=["prontuaRio"],
    )
}}


with 

source_ as (
  select *
  from {{source('brutos_prontuario_prontuaRIO', 'triagem') }}
),

triagem as (
      select
            json_extract_scalar(data, '$.prontuario') as id_prontuario,
            json_extract_scalar(data, '$.id_be') as id_boletim,
            json_extract_scalar(data, '$.co_internacao') as id_internacao,
            json_extract_scalar(data, '$.co_receituario') as id_receituario,
            json_extract_scalar(data, '$.ds_receituario') as descricao_receituario,
            json_extract_scalar(data, '$.cpf_profissional') as profissional_cpf,
            json_extract_scalar(data, '$.no_profissional') as profissional_nome,
            safe_cast(json_extract_scalar(data, '$.data_reg') as datetime) as data_registro,
            json_extract_scalar(data, '$.cpf_profissional_2') as profissional_cpf_2,
            json_extract_scalar(data, '$.no_profissional_2') as profissional_nome_2,
            json_extract_scalar(data, '$.ds_atividade') as descricao_atividade,
            json_extract_scalar(data, '$.dt_consulta_amb') as dt_consulta_amb,
            json_extract_scalar(data, '$.modulo_triagem') as modulo_triagem,
            json_extract_scalar(data, '$.obs_prescricao') as obs_prescricao,
            json_extract_scalar(data, '$.cor_risco') as cor_risco,
            json_extract_scalar(data, '$.pa_triagem') as pressao_arterial,
            json_extract_scalar(data, '$.fc_triagem') as frequencia_cardiaca,
            json_extract_scalar(data, '$.temp_triagem') as temperatura,
            json_extract_scalar(data, '$.peso_triagem') as peso,
            json_extract_scalar(data, '$.altura_triagem') as altura,
            json_extract_scalar(data, '$.spo2_triagem') as spo2,
            json_extract_scalar(data, '$.hgt_triagem') as hgt,
            json_extract_scalar(data, '$.diabetico_triagem') as diabetico,
            json_extract_scalar(data, '$.alergia_triagem') as alergia_quantidade,
            json_extract_scalar(data, '$.q_alergia_triagem') as alergia,
            json_extract_scalar(data, '$.hiper_triagem') as hiper,
            json_extract_scalar(data, '$.d_p_e_triagem') as doencas_pre_existentes_quantidade,
            json_extract_scalar(data, '$.q_d_p_e_triagem') as doencas_pre_existentes,
            json_extract_scalar(data, '$.m_u_c_triagem') as medicamento_uso_continuo_quantidade,
            json_extract_scalar(data, '$.q_m_u_c_triagem') as medicamento_uso_continuo,
            json_extract_scalar(data, '$.c_recentes_triagem') as quantidade_c_recentes,
            json_extract_scalar(data, '$.q_c_recentes_triagem') as c_recentes,
            json_extract_scalar(data, '$.queixas_triagem') as queixas,
            json_extract_scalar(data, '$.id_fluxograma') as id_fluxograma,
            json_extract_scalar(data, '$.id_discriminador') as id_discriminador,
            json_extract_scalar(data, '$.gesta_triagem') as gesta,
            json_extract_scalar(data, '$.para_triagem') as para,
            json_extract_scalar(data, '$.parto_triagem') as parto,
            json_extract_scalar(data, '$.aborto_triagem') as aborto,
            json_extract_scalar(data, '$.dum_triagem') as dum,
            json_extract_scalar(data, '$.dpp_triagem') as dpp,
            json_extract_scalar(data, '$.ig_triagem') as ig,
            json_extract_scalar(data, '$.prenatal_triagem') as prenatal,
            json_extract_scalar(data, '$.num_consultas_triagem') as numero_consultas,
            json_extract_scalar(data, '$.local_triagem') as local,
            json_extract_scalar(data, '$.hiv_triagem') as hiv,
            safe_cast(json_extract_scalar(data, '$.dt_obstetrica_triagem') as date)as dt_obstetrica_triagem,
            json_extract_scalar(data, '$.resultado_triagem') as resultado_triagem,
            safe_cast(json_extract_scalar(data, '$.tempo_acolh_triagem') as datetime) as tempo_acolhimento,
            safe_cast(json_extract_scalar(data, '$.tempo_atend_triagem') as datetime) as tempo_atendimento,
            json_extract_scalar(data, '$.status_fatu') as status_fatu,
            json_extract_scalar(data, '$.cpf_profissional_atend') as profissional_atendimento_cpf,
            json_extract_scalar(data, '$.parto_normal') as parto_normal,
            json_extract_scalar(data, '$.parto_cesario') as parto_cesario,
            cnes,
            loaded_at
      from source_
), 

deduplicated as (
  select * from triagem 
  qualify row_number() over(partition by id_prontuario, id_boletim, cnes order by loaded_at desc) = 1
)

select *, date(loaded_at) as data_particao
from deduplicated