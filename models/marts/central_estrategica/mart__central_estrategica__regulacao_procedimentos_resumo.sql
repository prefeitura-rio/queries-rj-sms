{{
  config(
    enabled=true,
    alias="regulacao_procedimentos_resumo",
    materialized='table',
  )
}}

/*
  Resumo por procedimento de regulação ambulatorial.
  Granularidade: um registro por procedimento_id.

  Métricas calculadas:
    - solicitacoes_criadas_30d: solicitações cuja data de criação está nos últimos 30 dias.
    - solicitacoes_marcadas_30d: solicitações com marcação agendada (marcacao.datahora)
      nos últimos 30 dias, independente de quando foram criadas.
    - percentual_falta_30d: proporção de faltas sobre o total de marcadas nos últimos 30
      dias. Calculado como faltas_30d / solicitacoes_marcadas_30d (nulo se marcadas = 0).
    - acumulados_na_fila: solicitações ativas (não canceladas nem negadas) criadas há
      mais de 30 dias e sem nenhuma marcação agendada — representa o estoque de espera.

  Definição de "ativa": detalhe_status não pertence ao conjunto de status terminais
  (CANCELADO, CANCELADA, NEGADA, NEGADO).

  A janela de 30 dias é calculada em relação à data atual (current_date()).

  Fonte: mart_regulacao__solicitacao + mart__central_estrategica__regulacao_procedimentos
*/

with
    -- Extrai a marcação mais relevante de cada solicitação:
    -- a última marcação com datahora preenchido.
    solicitacoes as (
        select
            s.solicitacao.id                                    as solicitacao_id,
            s.procedimento.id                                   as procedimento_id,
            date(s.solicitacao.solicitacao_datahora)            as data_criacao,
            upper(s.solicitacao.detalhe_status)                 as detalhe_status,
            (
                select m.datahora
                from unnest(s.marcacao) as m
                where m.datahora is not null
                order by m.datahora desc
                limit 1
            )                                                   as marcacao_datahora,
            (
                select m.flag_falta_registrada
                from unnest(s.marcacao) as m
                where m.datahora is not null
                order by m.datahora desc
                limit 1
            )                                                   as flag_falta_registrada
        from {{ ref('mart_regulacao__solicitacao') }} as s
        where s.procedimento.id is not null
    ),

    -- Agrega métricas por procedimento
    agregado as (
        select
            procedimento_id,

            -- Últimos 30 dias: criadas
            countif(
                data_criacao >= date_sub(current_date(), interval 30 day)
            )                                                   as solicitacoes_criadas_30d,

            -- Últimos 30 dias: marcadas (data do agendamento, não da criação)
            countif(
                date(marcacao_datahora) >= date_sub(current_date(), interval 30 day)
                and date(marcacao_datahora) <= current_date()
            )                                                   as solicitacoes_marcadas_30d,

            -- Últimos 30 dias: faltas (sobre as marcadas no período)
            countif(
                flag_falta_registrada = 'sim'
                and date(marcacao_datahora) >= date_sub(current_date(), interval 30 day)
                and date(marcacao_datahora) <= current_date()
            )                                                   as faltas_30d,

            -- Fila acumulada: ativas, criadas há mais de 30 dias, sem marcação
            countif(
                data_criacao < date_sub(current_date(), interval 30 day)
                and marcacao_datahora is null
                and detalhe_status not in ('CANCELADO', 'CANCELADA', 'NEGADA', 'NEGADO')
            )                                                   as acumulados_na_fila

        from solicitacoes
        group by procedimento_id
    )

select
    a.procedimento_id,
    p.procedimento_descricao,
    p.grupo_codigo,
    p.nome_grupo,
    a.solicitacoes_criadas_30d,
    a.solicitacoes_marcadas_30d,
    round(
        safe_divide(a.faltas_30d, a.solicitacoes_marcadas_30d) * 100,
        1
    )                                                           as percentual_falta_30d,
    a.acumulados_na_fila
from agregado as a
inner join {{ ref('mart__central_estrategica__regulacao_procedimentos') }} as p
    on a.procedimento_id = p.procedimento_id
order by a.solicitacoes_criadas_30d desc, p.procedimento_descricao
