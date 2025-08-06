with
profissionais_rio_historico as (
    select 
        -- identificacao
        ano_competencia as ano,
        mes_competencia as mes,
        id_cnes as unidade_id_cnes,
        cpf as profissional_cpf, --hashear

        -- atributos
        cbo_familia as profissional_ocupacao,
        cbo as profissional_ocupacao_especifica,
        vinculacao as profissional_vinculo,
        vinculo_tipo as profissional_vinculo_especifico,

        -- carga horaria
        carga_horaria_ambulatorial as profissional_carga_horaria_semanal_ambulatorial,
        carga_horaria_hospitalar as profissional_carga_horaria_semanal_hospitalar,
        carga_horaria_outros as profissional_carga_horaria_semanal_outros,
        carga_horaria_total as profissional_carga_horaria_semanal_total

    from {{ ref("dim_profissional_sus_rio_historico")}}
    where 1 = 1
        and ano_competencia >= 2022
        and ano_competencia < 2025
)

select * from profissionais_rio_historico
