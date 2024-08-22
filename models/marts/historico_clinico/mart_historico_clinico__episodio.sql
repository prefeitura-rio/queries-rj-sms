{{
    config(
        schema="saude_historico_clinico",
        alias="episodio_assistencial",
        materialized="table",
        cluster_by = "paciente_cpf",
    )
}}


with 
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    --  MERGING DATA: Merging Data from Different Sources
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    merged_data as (
        select
            paciente, 
            tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            null as prescricoes, -- VITAI source does not have prescription data
            estabelecimento, 
            profissional_saude_responsavel,
            prontuario,
            metadados
        from {{ ref("int_historico_clinico__episodio__vitai") }}
            union all
        select
            paciente, 
            tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            prescricoes,
            estabelecimento, 
            profissional_saude_responsavel,
            prontuario,
            metadados
        from {{ ref("int_historico_clinico__episodio__vitacare") }}
    ),
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    --  FINGERPRINT: Adding Unique Hashed Field
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    fingerprinted as (
        select 
            -- Patient Unique Identifier: for clustering purposes
            paciente.cpf as paciente_cpf,

            -- Encounter Unique Identifier: for testing purposes
            farm_fingerprint(concat(prontuario.fornecedor, prontuario.id_atendimento)) as id_atendimento,

            -- Encounter Data
            merged_data.*,
        from merged_data
    ),
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    --  EXHIBITION CONFIGURATION: Configuring Exhibition Rules
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- IS DATA FROM A MINOR?
    exhibition_minor_age as (
        select 
            fingerprinted.id_atendimento,
            safe_cast(
                case
                    when paciente.data_nascimento is null then false
                    when DATE_DIFF(current_date(), paciente.data_nascimento, YEAR) >= 18 then false
                    when DATE_DIFF(current_date(), paciente.data_nascimento, YEAR) < 18 then true
                end
            as boolean) as tem_exibicao_limitada,
            safe_cast(
                case
                    when paciente.data_nascimento is null then null
                    when DATE_DIFF(current_date(), paciente.data_nascimento, YEAR) >= 18 then null
                    when DATE_DIFF(current_date(), paciente.data_nascimento, YEAR) < 18 then "Menor de Idade"
                end
            as string) as motivo
        from fingerprinted
    ),
    -- IS THE PATIENT UNIDENTIFIED?
    exhibition_no_identifier as (
        select 
            fingerprinted.id_atendimento,
            safe_cast(
                case
                    when paciente.cpf is null and array_length(paciente.cns) = 0 then true
                    else false
                end
            as boolean) as tem_exibicao_limitada,
            safe_cast(
                case
                    when paciente.cpf is null and array_length(paciente.cns) = 0 then "Paciente sem CPF e CNS"
                    else null
                end
            as string) as motivo
        from fingerprinted
    ),
    -- IS THE EPISODE MISSING BASIC DATA?
    exhibition_missing_basic_data as (
        select 
            fingerprinted.id_atendimento,
            safe_cast(
                case
                    when 
                        array_length(fingerprinted.condicoes) = 0 and 
                        fingerprinted.motivo_atendimento is null and
                        fingerprinted.desfecho_atendimento is null
                        then true
                    else false
                end
            as boolean) as tem_exibicao_limitada,
            safe_cast(
                case
                    when 
                        array_length(fingerprinted.condicoes) = 0 and 
                        fingerprinted.motivo_atendimento is null and
                        fingerprinted.desfecho_atendimento is null
                        then "Episódio Não Informativo"
                    else null
                end
            as string) as motivo
        from fingerprinted
    ),
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    --  JOINING EXHIBITION RULES
    ---=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    exhibition_configurations as (
        select * from exhibition_minor_age
        union all
        select * from exhibition_no_identifier
        union all
        select * from exhibition_missing_basic_data
    ),
    exhibitions as (
        select 
            id_atendimento,
            struct(
                not(logical_or(tem_exibicao_limitada)) as indicador,
                array_agg(motivo ignore nulls) as motivos
            ) as exibicao
        from exhibition_configurations
        group by id_atendimento
    )

select 
    fingerprinted.*,
    exhibitions.exibicao
from fingerprinted
    inner join exhibitions using (id_atendimento)