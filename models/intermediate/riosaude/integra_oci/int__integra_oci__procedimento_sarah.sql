{{
    config(
        alias="procedimentos_sarah",
        materialized="table",
    )
}}

with

condicoes_principais as (
    select
        source_id as gid_boletim,
        cid_principal as cid,
        'principal' as fonte
    from {{ref('raw_prontuario_sarah__atendimento')}}
),

condicoes_secundarias as (
    select
        source_id as gid_boletim,
        cid_secundario as cid,
        'secundario' as fonte
    from {{ref('raw_prontuario_sarah__atendimento')}}
    where cid_secundario is not null
),

todos_cids as (
    select * from condicoes_principais
    union all
    select * from condicoes_secundarias
),

todos_cids_agrupados as (
    select
        gid_boletim,
        array_agg(
            struct(
                cid,
                fonte
            )
        ) as condicoes
    from todos_cids
    group by 1
),

procedimentos as (
    select
        source_id as gid_boletim,
        paciente_cpf,
        paciente_cns,
        paciente_nome,
        paciente_data_nascimento,
        safe_cast(null as string) as paciente_sexo,
        safe_cast(null as string) as paciente_raca_cor,
        id_cnes,
        atendimento_medico_datahora as momento,
        procedimento_codigo,
        profissional_cbo as cbo_profissional,
        profissional_nome as nome_profissional
    from {{ref('raw_prontuario_sarah__atendimento')}}
),


integracao as (
    select
        todos_cids_agrupados.condicoes as condicoes,
        procedimentos.procedimento_codigo as codigo_procedimento,
        procedimentos.momento as momento,
        procedimentos.id_cnes as cnes_estabelecimento,
        procedimentos.cbo_profissional,
        procedimentos.nome_profissional,
        procedimentos.paciente_cpf as cpf_paciente,
        procedimentos.paciente_cns as cns_paciente,
        procedimentos.paciente_nome as nome_paciente,
        procedimentos.paciente_raca_cor as raca_cor_paciente,
        procedimentos.paciente_data_nascimento as data_nascimento_paciente,
        procedimentos.paciente_sexo as sexo_paciente,
        struct(
            procedimentos.gid_boletim as id_boletim,
            procedimentos.paciente_cns as id_paciente
        ) as metadados
    from procedimentos
        left join todos_cids_agrupados on procedimentos.gid_boletim = todos_cids_agrupados.gid_boletim
    where
        procedimentos.id_cnes is not null
)
select * 
from integracao
where momento >= '2026-09-01'