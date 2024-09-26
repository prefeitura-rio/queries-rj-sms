{{
    config(
        alias="paciente",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with 
    todos_pacientes as (
        select
            *
        from {{ ref('mart_historico_clinico__paciente') }}
    ),
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    --  INFORMAÇÕES DE CADASTRO
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    unidade_de_cadastros_dos_pacientes as (
        select
            cpf, cadastro.id_cnes as id_cnes
        from todos_pacientes, unnest(prontuario) as cadastro
    ),
    unidades_saude as (
        select
            id_cnes, area_programatica
        from {{ ref('dim_estabelecimento') }}
    ),
    ap_de_cadastro_dos_pacientes as (
        select 
            distinct cpf, unidades_saude.area_programatica as ap
        from unidade_de_cadastros_dos_pacientes
            inner join unidades_saude using (id_cnes)
    ),
    ap_cadastro_por_paciente as (
        select
            cpf, array_agg(ap ignore nulls) as ap_cadastro
        from ap_de_cadastro_dos_pacientes
        group by cpf
    ),
    unidades_cadastro_por_paciente as (
        select
            cpf, array_agg(id_cnes ignore nulls) as unidades_cadastro
        from unidade_de_cadastros_dos_pacientes
        group by cpf
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
            not(logical_or(tem_exibicao_limitada)) as indicador,
            array_agg(motivo ignore nulls) as motivos
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
            todos_pacientes.cpf,
            todos_pacientes.cns[safe_offset(0)] as cns,
            safe_cast(dados.data_nascimento as string) as birth_date,
            dados.genero as gender,
            dados.raca as race,
            dados.obito_indicador as deceased,
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
            dados.identidade_validada_indicador as validated,
            safe_cast(todos_pacientes.cpf as int64) as cpf_particao
        from todos_pacientes
    )
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
--  JUNTANDO INFORMAÇÕES DE EXIBICAO
---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
select
    regras_exibicao.cpf,
    formatado.* except(cpf, cpf_particao),
    struct(
        regras_exibicao.indicador,
        regras_exibicao.motivos,
        ap_cadastro_por_paciente.ap_cadastro,
        unidades_cadastro_por_paciente.unidades_cadastro
    ) as exibicao,
    cpf_particao
from regras_exibicao
    left join formatado on (
        regras_exibicao.cpf = formatado.cpf and 
        regras_exibicao.indicador = true
    )
    left join ap_cadastro_por_paciente on ap_cadastro_por_paciente.cpf = regras_exibicao.cpf
    left join unidades_cadastro_por_paciente on unidades_cadastro_por_paciente.cpf = regras_exibicao.cpf