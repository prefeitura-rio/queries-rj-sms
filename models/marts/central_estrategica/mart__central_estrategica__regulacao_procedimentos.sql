{{
  config(
    enabled=true,
    alias="regulacao_procedimentos",
    materialized='table',
  )
}}

/*
  Lista de procedimentos distintos registrados nas solicitações de regulação.
  Granularidade: um registro por procedimento_id.

  O procedimento_id é a chave primária. A descrição normalizada é extraída de
  mart_historico_clinico_app__regulacao (já com padronização de abreviações e
  filtro de privacidade), associada ao procedimento via mart_regulacao__solicitacao.

  Em caso de múltiplas descrições para o mesmo procedimento_id, mantém a mais frequente.

  Fonte: mart_historico_clinico_app__regulacao + mart_regulacao__solicitacao
*/

with
    -- Associa cada solicitacao_id ao seu procedimento_id e descrição normalizada
    base as (
        select
            s.procedimento.id               as procedimento_id,
            s.procedimento.grupo_codigo     as grupo_codigo,
            s.procedimento.grupo_nome       as nome_grupo,
            r.procedimento_descricao
        from {{ ref('mart_historico_clinico_app__regulacao') }} as r
        inner join {{ ref('mart_regulacao__solicitacao') }} as s
            on r.solicitacao_id = s.solicitacao.id
        where
            s.procedimento.id is not null
            and r.procedimento_descricao is not null
    ),

    -- Conta frequência por (procedimento_id, descricao) para resolver conflitos de nome
    contagem as (
        select
            procedimento_id,
            grupo_codigo,
            nome_grupo,
            procedimento_descricao,
            count(*) as frequencia
        from base
        group by
            procedimento_id,
            grupo_codigo,
            nome_grupo,
            procedimento_descricao
    ),

    -- Para cada procedimento_id, mantém a descrição mais frequente
    ranked as (
        select
            procedimento_id,
            grupo_codigo,
            nome_grupo,
            procedimento_descricao,
            sum(frequencia) over (partition by procedimento_id) as total_solicitacoes,
            row_number() over (
                partition by procedimento_id
                order by frequencia desc, procedimento_descricao
            ) as rn
        from contagem
    )

select
    procedimento_id,
    procedimento_descricao,
    grupo_codigo,
    nome_grupo,
    total_solicitacoes
from ranked
where rn = 1
order by total_solicitacoes desc, procedimento_descricao
