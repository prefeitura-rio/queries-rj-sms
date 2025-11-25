{{
    config(
        alias='serie_temporal_vacinacoes',
        materialized='table',
    )
}}

WITH

    range_60_ultimos_dias as (
        SELECT GENERATE_DATE_ARRAY(
            CAST(CURRENT_DATE() - INTERVAL 60 DAY AS DATE), 
            CURRENT_DATE(), 
            INTERVAL 1 DAY
        ) AS dias
    ),
    todas_as_combinacoes_de_dia_e_cnes as (
        SELECT est.nome_limpo, est.area_programatica, est.id_cnes, date
        FROM range_60_ultimos_dias, UNNEST(range_60_ultimos_dias.dias) as date
            CROSS JOIN {{ ref("dim_estabelecimento") }} est
        WHERE est.prontuario_versao = 'vitacare'
            AND est.prontuario_episodio_tem_dado = 'sim'
    ),

    -- ------------------------------------------------------------
    -- Vacinacoes
    -- ------------------------------------------------------------
    vacinacoes as (
        SELECT
            id_vacinacao,
            id_cnes,
            particao_aplicacao_vacinacao as date
        FROM {{ ref("mart_cie__vacinacao") }}
    ),

    agrupamento_por_dia_e_cnes as (
        SELECT
            date,
            id_cnes,
            COUNT(*) as qtd_vacinacoes
        FROM vacinacoes
        GROUP BY 1, 2
    ),

    juncao_todas_as_combinacoes_de_dia_e_cnes as (
        SELECT 
            todas_as_combinacoes_de_dia_e_cnes.nome_limpo,
            todas_as_combinacoes_de_dia_e_cnes.area_programatica,
            todas_as_combinacoes_de_dia_e_cnes.id_cnes,
            todas_as_combinacoes_de_dia_e_cnes.date,
            COALESCE(agrupamento_por_dia_e_cnes.qtd_vacinacoes, 0) as qtd_vacinacoes
        FROM todas_as_combinacoes_de_dia_e_cnes
            LEFT JOIN agrupamento_por_dia_e_cnes using (id_cnes, date)
    )

select * 
from juncao_todas_as_combinacoes_de_dia_e_cnes
where date is not null
order by 1,2