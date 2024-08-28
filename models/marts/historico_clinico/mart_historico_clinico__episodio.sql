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
    ranked as (
        select
            *,
            row_number() over (partition by id_atendimento) as rank
        from fingerprinted
    )
select 
    *,
from ranked
where rank = 1