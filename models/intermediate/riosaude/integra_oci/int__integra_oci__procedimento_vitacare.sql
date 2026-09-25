{{
    config(
        alias="procedimentos_vitacare",
        materialized="table",
    )
}}

with

todos_cids as (
    select 
        id_prontuario_global, 
        cod_cid10 as cid,
        'principal' as fonte
    from {{ref('raw_prontuario_vitacare_historico__condicao')}} 
    where situacao in ('ATIVO', 'N.E')
),

todos_cids_agrupados as (
    select
        id_prontuario_global,
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
        todos_cids_agrupados.condicoes,
        p.co_procedimento as codigo_procedimento,
        coalesce(a.datahora_fim_atendimento, a.datahora_inicio_atendimento) as momento,
        a.id_cnes as cnes_estabelecimento,
        a.profissional_cbo as cbo_profissional,
        a.profissional_nome as nome_profissional,
        c.cpf as cpf_paciente,
        c.cns as cns_paciente,
        c.nome as nome_paciente,
        c.raca_cor as raca_cor_paciente,
        c.data_nascimento as data_nascimento_paciente,
        c.sexo as sexo_paciente,

        struct(
            p.id_prontuario_global as id_boletim,
            a.id_paciente_global as id_paciente
        ) as metadados
        
    from {{ref('raw_prontuario_vitacare_historico__procedimentos_clinicos')}} p
        inner join {{ref('raw_prontuario_vitacare_historico__acto')}} a using(id_prontuario_global)
        inner join {{ref('raw_prontuario_vitacare_historico__cadastro')}} c on c.id_global = a.id_paciente_global
        inner join todos_cids_agrupados using(id_prontuario_global)
    where co_procedimento in (
        '0301010064'
    )
)
select *
from procedimentos
where momento > '2026-09-01'