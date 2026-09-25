{{
    config(
        alias="agendamentos",
        materialized="table"
    )
}}


with

dim_estab as (
    select
        id_cnes,
        nome_limpo
    from {{ref('dim_estabelecimento')}}
),

-- Extrai o CNES do último executante antes de filtrar,
-- pois ORDINAL() não pode ser usado dentro de cláusula WHERE
base as (
    select
        paciente_cpf,
        paciente_cns,
        procedimento.sigtap_id,
        solicitante.unidade_id_cnes                                         as solicitante_cnes,
        solicitante.unidade_nome                                             as solicitante_nome_origem,
        execucao[safe_offset(array_length(execucao) - 1)].unidade_id_cnes  as executante_cnes
    from {{ref('mart_regulacao__solicitacao')}} s
    where
        cancelamento.datahora is null
        and s.solicitacao.solicitacao_datahora > '2026-09-01'
),

dados as (
    select *
    from base
    where
        solicitante_cnes in (select id_cnes from dim_estab)
        or executante_cnes in (select id_cnes from dim_estab)
),

final as (
    select
        d.paciente_cpf,
        d.paciente_cns,
        d.sigtap_id,

        -- Unidade solicitante
        d.solicitante_cnes                                          as solicitante_unidade_id_cnes,
        coalesce(es.nome_limpo, d.solicitante_nome_origem)         as solicitante_unidade_nome,

        -- Unidade executante
        d.executante_cnes                                           as executante_unidade_id_cnes,
        ee.nome_limpo                                               as executante_unidade_nome

    from dados d
    left join dim_estab es on es.id_cnes = d.solicitante_cnes
    left join dim_estab ee on ee.id_cnes = d.executante_cnes
)

select * from final
