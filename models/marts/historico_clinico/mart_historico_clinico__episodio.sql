{{
    config(
        schema="saude_historico_clinico",
        alias="episodio_assistencial",
        materialized="table",
        cluster_by="paciente_cpf",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}


with
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- MERGING DATA: Merging Data from Different Sources
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    merged_data as (
        select
            paciente,
            tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            exames_realizados,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            null as prescricoes,  -- VITAI source does not have prescription data
            estabelecimento,
            profissional_saude_responsavel,
            prontuario,
            metadados,
            cpf_particao,
        from {{ ref("int_historico_clinico__episodio__vitai") }}
        union all
        select
            paciente,
            tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            array(
                select as struct
                    cast(null as string) as tipo, cast(null as string) as descricao
            ) as exames_realizados,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            prescricoes,
            estabelecimento,
            profissional_saude_responsavel,
            prontuario,
            metadados,
            cpf_particao,
        from {{ ref("int_historico_clinico__episodio__vitacare") }}
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FINGERPRINT: Adding Unique Hashed Field
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    fingerprinted as (
        select
            -- Patient Unique Identifier: for clustering purposes
            paciente.cpf as paciente_cpf,

            -- Encounter Unique Identifier: for testing purposes
            farm_fingerprint(
                concat(prontuario.fornecedor, prontuario.id_atendimento)
            ) as id_atendimento,

            -- Encounter Data
            merged_data.*,
        from merged_data
    ),
    deduped as (
        select *
        from fingerprinted
        qualify row_number() over (partition by id_atendimento) = 1
    )
select *
from deduped

