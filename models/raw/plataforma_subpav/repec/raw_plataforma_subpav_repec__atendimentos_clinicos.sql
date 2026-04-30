{{
    config(
        alias="repec__atendimentos_clinicos",
        materialized="table",
        tags=["subpav", "repec"]
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_subpav_staging", "subpav_repec__atendimentos_clinicos") }}
    ),

    tratar_campos as (
        select
            {{ process_null('id_atendimento') }} as id_atendimento,
            {{ process_null('cnes_entidade') }} as cnes_entidade,
            {{ process_null('id_paciente') }} as id_paciente,
            {{ process_null('id_profissional') }} as id_profissional,
            {{ process_null('id_agendamento') }} as id_agendamento,
            trim({{ remove_decode_chars_error("cast(" ~ process_null('tipo_atendimento') ~ " as string)") }}) as tipo_atendimento,
            {{ process_null('data_atendimento') }} as data_atendimento,
            {{ process_null('data_registro') }} as data_registro,
            {{ process_null('saude_infantil') }} as saude_infantil,
            {{ process_null('saude_juvenil') }} as saude_juvenil,
            {{ process_null('plan_familiar') }} as plan_familiar,
            {{ process_null('saude_materna') }} as saude_materna,
            {{ process_null('revisao_puerperio') }} as revisao_puerperio,
            {{ process_null('rccu') }} as rccu,
            {{ process_null('rcm') }} as rcm,
            {{ process_null('rccr') }} as rccr,
            {{ process_null('hipertensao') }} as hipertensao,
            {{ process_null('diabetes') }} as diabetes,
            {{ process_null('inr') }} as inr,
            {{ process_null('dependentes') }} as dependentes,
            {{ process_null('motivo_icpc') }} as motivo_icpc,
            {{ process_null('motivo_observacoes') }} as motivo_observacoes,
            {{ process_null('plano_icpc') }} as plano_icpc,
            {{ process_null('plano_observacoes') }} as plano_observacoes,
            {{ process_null('observacoes_gerais') }} as observacoes_gerais,
            {{ process_null('origem_arquivo') }} as origem_arquivo,
            {{ process_null('origem_banco') }} as origem_banco,
            {{ repec_origem_unidade_para_cnes("origem_unidade") }} as cnes_origem,
            safe_cast({{ process_null('datalake_loaded_at') }} as timestamp) as datalake_loaded_at
        from source
    ),

    deduplicar as (
        select *
        from tratar_campos
        qualify row_number() over (
            partition by
                    id_atendimento,
                    cnes_entidade,
                    id_paciente,
                    id_profissional,
                    id_agendamento,
                    tipo_atendimento,
                    data_atendimento,
                    data_registro,
                    saude_infantil,
                    saude_juvenil,
                    plan_familiar,
                    saude_materna,
                    revisao_puerperio,
                    rccu,
                    rcm,
                    rccr,
                    hipertensao,
                    diabetes,
                    inr,
                    dependentes,
                    motivo_icpc,
                    motivo_observacoes,
                    plano_icpc,
                    plano_observacoes,
                    observacoes_gerais
            order by
                    datalake_loaded_at desc,
                    origem_arquivo desc
        ) = 1
    )

select *
from deduplicar
