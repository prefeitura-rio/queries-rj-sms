{{
    config(
        alias="procedimentos_vitai",
        materialized="table",
    )
}}

with

exames as (
    select 
        gid_boletim,
        gid_paciente,
        gid_estabelecimento,
        gid_medico_solicitante,
        coalesce(realizacao_data, liberacao_data, pedido_data) as momento,
        procedimento_codigo
    from {{ ref('raw_prontuario_vitai__exame') }}
),

cids_altas as (
    select 
        gid_boletim,
        cid_codigo_alta as cid,
        'alta' as fonte
    from {{ ref('raw_prontuario_vitai__resumo_alta') }}
),

cids_diagnostico as (
    select 
        gid_boletim,
        codigo as cid,
        'diagnostico' as fonte
    from {{ ref('raw_prontuario_vitai__diagnostico') }}
),

cids_atendimento as (
    select 
        gid_boletim,
        cid_codigo as cid,
        'atendimento' as fonte
    from {{ ref('raw_prontuario_vitai__atendimento') }}
),

cids_internacao as (
    select 
        gid_boletim,
        id_diagnostico as cid,
        'internacao' as fonte
    from {{ ref('raw_prontuario_vitai__internacao') }}
),

todos_cids as (
    select * from cids_internacao
    union all
    select * from cids_atendimento
    union all
    select * from cids_altas
    union all
    select * from cids_diagnostico
),

cids_agrupados as (
    select
        gid_boletim,
        array_agg(
            struct(
                cid,
                fonte
            )
        ) as condicoes
    from todos_cids
    where (cid != '') and (cid is not null)
    group by 1
),

estabelecimentos as (
    select 
        gid,
        cnes
    from {{ ref('raw_prontuario_vitai__m_estabelecimento') }}
),

profissionais as (
    select 
        gid,
        nome,
        cbo
    from {{ ref('raw_prontuario_vitai__profissional') }}
),

pacientes as (
    select
        gid,
        cpf,
        cns,
        nome,
        raca_cor,
        data_nascimento,
        sexo
    from {{ ref('raw_prontuario_vitai__paciente') }}
),

integracao as (
    select
        cids_agrupados.condicoes as condicoes,
        exames.procedimento_codigo as codigo_procedimento,
        exames.momento as momento,
        estabelecimentos.cnes as cnes_estabelecimento,
        profissionais.cbo as cbo_profissional,
        profissionais.nome as nome_profissional,
        pacientes.cpf as cpf_paciente,
        pacientes.cns as cns_paciente,
        pacientes.nome as nome_paciente,
        pacientes.raca_cor as raca_cor_paciente,
        pacientes.data_nascimento as data_nascimento_paciente,
        pacientes.sexo as sexo_paciente,
        struct(
            exames.gid_boletim as id_boletim,
            exames.gid_paciente as id_paciente
        ) as metadados
    from exames
        left join estabelecimentos on exames.gid_estabelecimento = estabelecimentos.gid
        left join profissionais on exames.gid_medico_solicitante = profissionais.gid
        left join pacientes on exames.gid_paciente = pacientes.gid
        left join cids_agrupados on exames.gid_boletim = cids_agrupados.gid_boletim
    where
        estabelecimentos.cnes is not null
)
select * 
from integracao
where momento >= '2026-09-01'