{{
  config(
    enabled=true,
    alias="regulacao_procedimentos",
    materialized='table',
  )
}}

/*
  Lista de procedimentos distintos registrados nas solicitações de regulação.
  Granularidade: um registro por código SIGTAP (sigtap_id).

  O código SIGTAP é a chave primária. A descrição normalizada é extraída de
  mart_historico_clinico_app__regulacao (já com padronização de abreviações e
  filtro de privacidade), associada ao SIGTAP via mart_regulacao__solicitacao.

  Em caso de múltiplas descrições para o mesmo SIGTAP, mantém a mais frequente.

  Fonte: mart_historico_clinico_app__regulacao + mart_regulacao__solicitacao
*/

with
    -- Associa cada solicitacao_id ao seu sigtap_id e descrição normalizada
    base as (
        select
            s.procedimento.sigtap_id        as sigtap_id,
            s.procedimento.grupo_codigo     as sigtap_grupo_codigo,
            s.procedimento.grupo_nome       as nome_grupo,
            r.procedimento_descricao
        from {{ ref('mart_historico_clinico_app__regulacao') }} as r
        inner join {{ ref('mart_regulacao__solicitacao') }} as s
            on r.solicitacao_id = s.solicitacao.id
        where
            s.procedimento.sigtap_id is not null
            and r.procedimento_descricao is not null
    ),

    -- Conta frequência por (sigtap_id, descricao) para resolver conflitos de nome
    contagem as (
        select
            sigtap_id,
            sigtap_grupo_codigo,
            nome_grupo,
            procedimento_descricao,
            count(*) as frequencia
        from base
        group by
            sigtap_id,
            sigtap_grupo_codigo,
            nome_grupo,
            procedimento_descricao
    ),

    -- Para cada sigtap_id, mantém a descrição mais frequente
    ranked as (
        select
            sigtap_id,
            sigtap_grupo_codigo,
            nome_grupo,
            procedimento_descricao,
            sum(frequencia) over (partition by sigtap_id) as total_solicitacoes,
            row_number() over (
                partition by sigtap_id
                order by frequencia desc, procedimento_descricao
            ) as rn
        from contagem
    )

select
    sigtap_id,
    procedimento_descricao,
    sigtap_grupo_codigo,
    nome_grupo,
    total_solicitacoes
from ranked
where rn = 1
order by total_solicitacoes desc, procedimento_descricao
