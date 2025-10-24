{{
    config(
        alias='serie_temporal_atendimentos',
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
        SELECT est.nome_limpo, est.area_programatica, est.id_cnes, data
        FROM range_60_ultimos_dias, UNNEST(range_60_ultimos_dias.dias) as data
            CROSS JOIN {{ ref("dim_estabelecimento") }} est
        WHERE est.prontuario_versao = 'vitacare'
            AND est.prontuario_episodio_tem_dado = 'sim'
    ),

    -- ------------------------------------------------------------
    -- Atendimentos
    -- ------------------------------------------------------------
    atendimentos as (
        SELECT
            data_particao as data,
            id_hci,
            estabelecimento.id_cnes as estabelecimento_id_cnes,
            estabelecimento.nome as estabelecimento_nome,
            prontuario.fornecedor as fornecedor_prontuario
        FROM {{ ref("mart_historico_clinico__episodio") }}
        WHERE 
            data_particao > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 60 DAY) AND
            data_particao < CURRENT_DATETIME() AND
            estabelecimento.id_cnes is not null
    ),

    agrupados_por_dia_e_cnes as (
        SELECT
            estabelecimento_id_cnes,
            estabelecimento_nome,
            fornecedor_prontuario,
            data,
            COUNT(*) as qtd_atendimentos
        FROM atendimentos
        GROUP BY 1, 2, 3, 4
    )

select * 
from agrupados_por_dia_e_cnes
order by 1,2,3