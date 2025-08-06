{{
    config(
        schema="intermediario_historico_clinico",
        alias="vacinacao_historico",
        materialized="table",
    )
}}

with

    source_vacina as (
        select 
            id_prontuario_global,
            id_cnes,
            nome_vacina,
            cod_vacina,
            dose,
            data_aplicacao,
            data_registro
        from {{ ref('raw_prontuario_vitacare_historico__vacina') }} 
    ),

    source_atendimento as (
        select 
            id_prontuario_global,
            patient_cpf
        from {{ ref('raw_prontuario_vitacare_historico__acto') }} 
    ),

    source_cadastro as (
        select 
            cpf,
            nome,
            nome_mae,
            data_nascimento
        from {{ ref('raw_prontuario_vitacare_historico__cadastro') }} 
    ),

    vacinas as (
       select
            sv.id_cnes as cnes_unidade,
            sc.cpf as cpf,
            sc.data_nascimento as data_nascimento,
            sc.nome_mae as nome_mae,
            sv.nome_vacina as nome_vacina,
            sv.dose as dose,
            sv.data_aplicacao as data_aplicacao,
            safe_cast(sv.data_registro as date) as data_registro
        from source_vacina sv
        left join source_atendimento sa on sv.id_prontuario_global = sa.id_prontuario_global
        left join source_cadastro sc on sa.patient_cpf = sc.cpf
    )

select * from vacinas