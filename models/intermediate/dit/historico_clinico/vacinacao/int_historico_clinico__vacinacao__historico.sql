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
            cpf
        from {{ ref('raw_prontuario_vitacare__atendimento') }} 
    ),

    source_cadastro as (
        select 
            cpf,
            nome,
            mae_nome,
            data_nascimento
        from {{ ref('raw_prontuario_vitacare__paciente') }} 
    ),

    vacinas as (
       select
            sv.id_cnes as cnes_unidade,
            sc.cpf as cpf,
            {{ proper_br('sc.nome') }} as nome,
            sc.data_nascimento as data_nascimento,
            {{ proper_br('sc.mae_nome') }} as nome_mae,
            lower({{ remove_accents_upper('sv.nome_vacina') }}) as nome_vacina,
            case
              when sv.dose = '1ª Dose' then '1 dose'
              when sv.dose = '2ª Dose' then '2 dose'
              when sv.dose = '3ª Dose' then '3 dose'
              when sv.dose = '4ª Dose' then '4 dose'
              when sv.dose = '5ª Dose' then '5 dose'
              when sv.dose = 'Dose única' then 'dose unica'
              when sv.dose = 'Dose adicional' then 'dose adicional'
              when sv.dose = 'Dose inicial' then 'dose inicial'
              when sv.dose = 'Revacinação' then 'revacinacao'
              when sv.dose = 'Reforço' then 'reforco'
              when sv.dose = '1º Reforço' then '1 reforco'
              when sv.dose = '2º Reforço' then '2 reforco'
              when sv.dose = '3º Reforço' then '3 reforco'
              when sv.dose = 'Dose D' then 'dose d'
              when sv.dose = 'Outro' then 'outra'
              else lower(sv.dose) 
            end as dose,
            sv.data_aplicacao as data_aplicacao,
            safe_cast(sv.data_registro as date) as data_registro
        from source_vacina sv
        left join source_atendimento sa on sv.id_prontuario_global = sa.id_prontuario_global
        left join source_cadastro sc on sa.cpf = sc.cpf
    )

select * from vacinas