{{
    config(
        materialized = "table",
        alias = "habilitacao_historico"
    )
}}
with
habilitacoes_rio_historico as (
    select
        ano_competencia as ano,
        mes_competencia as mes,
        id_cnes as unidade_id_cnes,

        habilitacao,
        habilitacao_ano_inicio,
        habilitacao_ano_fim,
        habilitacao_mes_inicio,
        habilitacao_mes_fim,
        habilitacao_ativa_indicador

    from {{ source("saude_cnes","habilitacao_sus_rio_historico")}}
    where 1 = 1
        and ano_competencia >= 2022
        and ano_competencia < 2025
)

select * from habilitacoes_rio_historico
where habilitacao is not null
