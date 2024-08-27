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
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    --  REGRAS DE EXIBIÇÃO
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- Regra 1: Menor de Idade
    regra_menor_de_idade as (
        select 
            todos_pacientes.cpf,
            safe_cast(
                case
                    when todos_pacientes.dados.data_nascimento is null then false
                    when DATE_DIFF(current_date(), todos_pacientes.dados.data_nascimento, YEAR) >= 18 then false
                    when DATE_DIFF(current_date(), todos_pacientes.dados.data_nascimento, YEAR) < 18 then true
                end
            as boolean) as tem_exibicao_limitada,
            safe_cast(
                case
                    when todos_pacientes.dados.data_nascimento is null then null
                    when DATE_DIFF(current_date(), todos_pacientes.dados.data_nascimento, YEAR) >= 18 then null
                    when DATE_DIFF(current_date(), todos_pacientes.dados.data_nascimento, YEAR) < 18 then "Menor de Idade"
                end
            as string) as motivo
        from todos_pacientes
    ),
    -- Juntando Regras
    todas_regras as (
        select * from regra_menor_de_idade
        -- union all
        -- (...)
    ),
    -- Agrupando Regras
    regras_exibicao as (
        select 
            cpf,
            struct(
                not(logical_or(tem_exibicao_limitada)) as indicador,
                array_agg(motivo ignore nulls) as motivos
            ) as exibicao
        from todas_regras
        group by cpf
    ),
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    --  FORMATAÇÃO
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    formatado as (
        select 
            dados.nome as registration_name,
            dados.nome_social as social_name,
            cpf,
            cns[safe_offset(0)] as cns,
            safe_cast(dados.data_nascimento as string) as birth_date,
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
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  JUNTANDO INFORMAÇÕES DE EXIBICAO
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select
    regras_exibicao.cpf,
    formatado.* except(cpf),
    regras_exibicao.exibicao
from regras_exibicao
    left join formatado on (
        regras_exibicao.cpf = formatado.cpf and 
        regras_exibicao.exibicao.indicador = true
    )