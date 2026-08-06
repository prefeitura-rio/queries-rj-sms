{{
  config(
    enabled=true,
    alias="regulacao_fila_historico",
    materialized='table',
    partition_by={"field": "ano_mes", "data_type": "date", "granularity": "month"},
    cluster_by=["procedimento_id"],
  )
}}

/*
  Série histórica mensal de procedimentos em fila de regulação ambulatorial.
  Granularidade: um registro por (ano_mes, procedimento_id).

  Definição de "em fila" no mês M (critério retroativo):
    Uma solicitação é contabilizada na fila de um mês M se, no último dia de M:
      1. Já havia sido criada (data_criacao <= último dia do mês M).
      2. Não possuía nenhuma marcação agendada até o final do mês M
         (marcacao_datahora is null ou marcacao_datahora > último dia do mês M).
      3. Não estava em status terminal: detalhe_status não pertence ao conjunto
         (CANCELADO, CANCELADA, NEGADA, NEGADO).
      4. Havia sido criada há mais de 30 dias em relação ao fim do mês M
         (data_criacao < último dia do mês M - 30 dias) — mesmo critério de
         "acumulado na fila" adotado em regulacao_procedimentos_resumo.

  Nota: detalhe_status é um snapshot do estado atual. Solicitações que foram
  canceladas/negadas após o mês de referência aparecem corretamente naquele
  mês, mas solicitações canceladas antes terão o cancelamento refletido em
  todos os meses — limitação inerente a dados sem histórico de status.

  Fonte: mart_regulacao__solicitacao + mart__central_estrategica__regulacao_procedimentos
*/

with
    -- Intervalo de meses a gerar: do mês mais antigo de solicitação até o mês atual
    meses as (
        select
            date_trunc(min(date(s.solicitacao.solicitacao_datahora)), month) as primeiro_mes
        from {{ ref('mart_regulacao__solicitacao') }} as s
        where s.solicitacao.solicitacao_datahora is not null
    ),

    spine_meses as (
        select
            date_trunc(mes, month)                              as ano_mes,
            last_day(mes, month)                                as fim_mes
        from
            meses,
            unnest(
                generate_date_array(
                    primeiro_mes,
                    date_trunc(current_date(), month),
                    interval 1 month
                )
            ) as mes
    ),

    -- Extrai atributos relevantes de cada solicitação
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
            )                                                   as marcacao_datahora
        from {{ ref('mart_regulacao__solicitacao') }} as s
        where s.procedimento.id is not null
    ),

    -- Para cada mês, conta quantas solicitações estavam em fila naquele momento
    fila_por_mes as (
        select
            sm.ano_mes,
            sol.procedimento_id,
            countif(
                -- Critério 1: solicitação já existia no fim do mês
                sol.data_criacao <= sm.fim_mes
                -- Critério 2: sem marcação até o fim do mês
                and (
                    sol.marcacao_datahora is null
                    or date(sol.marcacao_datahora) > sm.fim_mes
                )
                -- Critério 3: não está em status terminal
                and sol.detalhe_status not in ('CANCELADO', 'CANCELADA', 'NEGADA', 'NEGADO')
                -- Critério 4: criada há mais de 30 dias em relação ao fim do mês
                and sol.data_criacao < date_sub(sm.fim_mes, interval 30 day)
            )                                                   as acumulados_na_fila
        from spine_meses as sm
        cross join solicitacoes as sol
        group by sm.ano_mes, sol.procedimento_id
    )

select
    f.ano_mes,
    f.procedimento_id,
    p.procedimento_descricao,
    p.grupo_codigo,
    p.nome_grupo,
    f.acumulados_na_fila
from fila_por_mes as f
inner join {{ ref('mart__central_estrategica__regulacao_procedimentos') }} as p
    on f.procedimento_id = p.procedimento_id
where f.acumulados_na_fila > 0
