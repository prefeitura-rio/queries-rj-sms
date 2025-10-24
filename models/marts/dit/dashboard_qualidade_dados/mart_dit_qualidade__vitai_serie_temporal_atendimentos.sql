{{
    config(
        alias='vitai_serie_temporal_atendimentos',
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
        WHERE est.prontuario_versao = 'vitai'
            AND est.prontuario_episodio_tem_dado = 'sim'
    ),

    atendimentos_basecentral as (
        SELECT
            est.cnes as id_cnes,
            bol.gid as id_prontuario_local,
            bol.updated_at
        FROM {{ ref("raw_prontuario_vitai__boletim") }} bol
            LEFT JOIN {{ ref("raw_prontuario_vitai__m_estabelecimento") }} est on bol.gid_estabelecimento = est.gid
        WHERE 
            bol.updated_at > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 60 DAY) AND
            bol.updated_at < CURRENT_DATETIME()
    ),

    atendimentos_api as (
        SELECT
            est.cnes as id_cnes,
            bol.id as id_prontuario_local,
            bol.updated_at
        FROM {{ ref("raw_prontuario_vitai_api__boletim") }} bol
            LEFT JOIN {{ ref("raw_prontuario_vitai_api__estabelecimento") }} est on bol.estabelecimento_id = est.id
        WHERE 
            bol.updated_at > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 60 DAY) AND
            bol.updated_at < CURRENT_DATETIME()
    ),

    api_agrupados_por_dia_e_cnes as (
        SELECT
            id_cnes,
            DATE(updated_at) as data,
            COUNT(*) as qtd_atendimentos
        FROM atendimentos_api
        GROUP BY 1, 2
    ),
    basecentral_agrupados_por_dia_e_cnes as (
        SELECT
            id_cnes,
            DATE(updated_at) as data,
            COUNT(*) as qtd_atendimentos
        FROM atendimentos_basecentral
        GROUP BY 1, 2
    ),

    juncao as (
        SELECT
            todas_as_combinacoes_de_dia_e_cnes.nome_limpo,
            todas_as_combinacoes_de_dia_e_cnes.area_programatica,
            todas_as_combinacoes_de_dia_e_cnes.id_cnes,
            todas_as_combinacoes_de_dia_e_cnes.data,

            COALESCE(api.qtd_atendimentos, 0) as qtd_atendimentos_api,
            COALESCE(basecentral.qtd_atendimentos, 0) as qtd_atendimentos_historico
        FROM todas_as_combinacoes_de_dia_e_cnes
            LEFT JOIN api_agrupados_por_dia_e_cnes api using (id_cnes, data)
            LEFT JOIN basecentral_agrupados_por_dia_e_cnes basecentral using (id_cnes, data)
    )

select * 
from juncao
order by 1,2,3,4