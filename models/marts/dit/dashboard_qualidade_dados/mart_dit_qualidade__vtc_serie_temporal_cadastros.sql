{{
    config(
        alias='vtc_serie_temporal_cadastros',
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

    cadastros_api as (
        SELECT
            id_cnes,
            ut_id,
            updated_at
        FROM {{ ref("raw_prontuario_vitacare_api__cadastro") }}
        WHERE 
            updated_at > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 60 DAY) AND
            updated_at < CURRENT_DATETIME()
    ),

    cadastros_historico as (
        SELECT
            id_cnes,
            id_local as ut_id,
            updated_at
        FROM {{ ref("raw_prontuario_vitacare_historico__cadastro") }}
        WHERE 
            updated_at > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 60 DAY) AND
            updated_at < CURRENT_DATETIME()
    ),

    api_agrupados_por_dia_e_cnes as (
        SELECT
            id_cnes,
            DATE(updated_at) as data,
            COUNT(*) as qtd_cadastros
        FROM cadastros_api
        GROUP BY 1, 2
    ),
    historico_agrupados_por_dia_e_cnes as (
        SELECT
            id_cnes,
            DATE(updated_at) as data,
            COUNT(*) as qtd_cadastros
        FROM cadastros_historico
        GROUP BY 1, 2
    ),

    juncao as (
        SELECT
            todas_as_combinacoes_de_dia_e_cnes.nome_limpo,
            todas_as_combinacoes_de_dia_e_cnes.area_programatica,
            todas_as_combinacoes_de_dia_e_cnes.id_cnes,
            todas_as_combinacoes_de_dia_e_cnes.data,

            COALESCE(api.qtd_cadastros, 0) as qtd_cadastros_api,
            COALESCE(historico.qtd_cadastros, 0) as qtd_cadastros_historico
        FROM todas_as_combinacoes_de_dia_e_cnes
            LEFT JOIN api_agrupados_por_dia_e_cnes api using (id_cnes, data)
            LEFT JOIN historico_agrupados_por_dia_e_cnes historico using (id_cnes, data)
    )

select * 
from juncao
order by 1,2,3,4