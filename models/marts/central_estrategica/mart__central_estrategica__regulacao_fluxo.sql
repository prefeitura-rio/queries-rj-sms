{{
  config(
    enabled=true,
    alias="regulacao_fluxo",
    materialized='table',
  )
}}

/*
  Fluxo de regulação entre unidades solicitantes e executantes por procedimento.
  Granularidade: (procedimento_id, cnes_solicitante, cnes_executante).

  Contabiliza a quantidade de solicitações que percorreram cada combinação
  (procedimento, solicitante → executante). Coordenadas geográficas e informações
  adicionais das unidades são obtidas via join com regulacao_unidades.

  Filtro temporal: apenas solicitações com marcação agendada nos últimos 30 dias
  (marcacao.datahora — última marcação com datahora preenchido). Reflete os fluxos
  que produziram agendamentos efetivos no período.

  Fonte: mart_historico_clinico_app__regulacao + mart_regulacao__solicitacao
*/

with
    -- Extrai (procedimento, solicitante, executante, marcacao_datahora) de cada solicitação
    pares as (
        select
            s.procedimento.id                                   as procedimento_id,
            r.procedimento_descricao,
            s.solicitante.unidade_id_cnes                       as cnes_solicitante,
            (
                select ex.unidade_id_cnes
                from unnest(s.execucao) as ex
                where ex.unidade_id_cnes is not null
                limit 1
            )                                                   as cnes_executante,
            (
                select m.datahora
                from unnest(s.marcacao) as m
                where m.datahora is not null
                order by m.datahora desc
                limit 1
            )                                                   as marcacao_datahora
        from {{ ref('mart_historico_clinico_app__regulacao') }} as r
        inner join {{ ref('mart_regulacao__solicitacao') }} as s
            on r.solicitacao_id = s.solicitacao.id
        where
            s.solicitante.unidade_id_cnes is not null
            and s.procedimento.id is not null
    ),

    -- Agrega quantidade por (procedimento, par solicitante-executante)
    final as (
        select
            procedimento_id,
            procedimento_descricao,
            cnes_solicitante,
            cnes_executante,
            count(*)                                            as quantidade_solicitacoes
        from pares
        where
            cnes_executante is not null
            and date(marcacao_datahora)
                >= date_sub(current_date(), interval 30 day)
        group by
            procedimento_id,
            procedimento_descricao,
            cnes_solicitante,
            cnes_executante
    )

select * from final
order by quantidade_solicitacoes desc
