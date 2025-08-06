with
equipamentos_rio_historico as (
    select
        ano_competencia as ano,
        mes_competencia as mes,
        id_cnes as unidade_id_cnes,
        equipamento,
        equipamento_especifico,
        equipamentos_quantidade,
        equipamentos_quantidade_ativos

    from {{ ref("dim_equipamento_sus_rio_historico")}}
    where 1 = 1
        and ano_competencia >= 2022
        and ano_competencia < 2025
)

select * from equipamentos_rio
