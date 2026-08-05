{{
  config(
    enabled=true,
    alias="regulacao_procedimentos",
    materialized='table',
  )
}}

/*
  Lista de procedimentos existentes no SISREG (regulação ambulatorial).
  Granularidade: um registro por procedimento SIGTAP distinto.
  Fonte: raw_sisreg_api_v2__solicitacao_ambulatorial
*/

with
    procedimentos as (
        select
            procedimento_sigtap_id                          as sigtap_procedimento,
            coalesce(
                procedimento_sigtap_descricao,
                procedimento_descricao
            )                                               as nome_procedimento,
            procedimento_grupo_codigo                       as sigtap_grupo_codigo,
            procedimento_grupo_nome                         as nome_grupo
        from {{ ref('raw_sisreg_api_v2__solicitacao_ambulatorial') }}
        where procedimento_sigtap_id is not null
    ),

    -- Pré-agrega para contar frequência por combinação
    contagem as (
        select
            sigtap_procedimento,
            nome_procedimento,
            sigtap_grupo_codigo,
            nome_grupo,
            count(*)                                        as frequencia
        from procedimentos
        group by
            sigtap_procedimento,
            nome_procedimento,
            sigtap_grupo_codigo,
            nome_grupo
    ),

    -- Garante unicidade por sigtap_procedimento:
    -- em caso de conflito de nomes/grupos, mantém o valor mais frequente
    ranked as (
        select
            sigtap_procedimento,
            nome_procedimento,
            sigtap_grupo_codigo,
            nome_grupo,
            row_number() over (
                partition by sigtap_procedimento
                order by frequencia desc
            )                                               as rn
        from contagem
    )

select
    sigtap_procedimento,
    nome_procedimento,
    sigtap_grupo_codigo,
    nome_grupo
from ranked
where rn = 1
order by nome_grupo, nome_procedimento
