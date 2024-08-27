{{
    config(
        alias="paciente",
        materialized="table",
        cluster_by="cpf",
        schema="app_historico_clinico",
    )
}}

with 
    todos_pacientes as (
        select
            *
        from {{ ref('mart_historico_clinico__paciente') }}
    ),
    formatado as (
        select 
            dados.nome as registration_name,
            dados.nome_social as social_name,
            cpf,
            cns[safe_offset(0)] as cns,
            dados.data_nascimento as birth_date,
            dados.genero as gender,
            dados.raca as race,
            contato.telefone[safe_offset(0)].valor as phone,
            struct(
                equipe_saude_familia[safe_offset(0)].clinica_familia.id_cnes as cnes,
                equipe_saude_familia[safe_offset(0)].clinica_familia.nome as name,
                equipe_saude_familia[safe_offset(0)].clinica_familia.telefone as phone
            ) as family_clinic,
            struct(
                equipe_saude_familia[safe_offset(0)].id_ine as ine_code,
                equipe_saude_familia[safe_offset(0)].nome as name,
                equipe_saude_familia[safe_offset(0)].telefone as phone
            ) as family_health_team,
            array(
                select 
                struct(
                    id_profissional_sus as registry,
                    nome as name
                )
                from unnest(equipe_saude_familia[safe_offset(0)].medicos)
            ) as medical_responsible,
            array(
                select 
                struct(
                    id_profissional_sus as registry,
                    nome as name
                )
                from unnest(equipe_saude_familia[safe_offset(0)].enfermeiros)
            ) as nursing_responsible,
            dados.identidade_validada_indicador as validated
        from todos_pacientes
    )

select
    *
from formatado