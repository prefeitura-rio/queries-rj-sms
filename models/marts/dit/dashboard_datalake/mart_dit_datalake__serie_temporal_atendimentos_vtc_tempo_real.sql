{{
    config(
        alias='serie_temporal_atendimentos_vtc_tempo_real',
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

    atendimentos_api as (
        SELECT
            id_cnes,
            id_prontuario_local,
            datahora_fim_atendimento as updated_at
        FROM {{ ref("raw_prontuario_vitacare_api__acto") }}
        WHERE 
            datahora_fim_atendimento > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 60 DAY) AND
            datahora_fim_atendimento < CURRENT_DATETIME()
    ),

    atendimentos_historico as (
        SELECT
            id_cnes,
            id_prontuario_local,
            datahora_fim_atendimento as updated_at
        FROM {{ ref("raw_prontuario_vitacare_historico__acto") }}
        WHERE 
            datahora_fim_atendimento > DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 60 DAY) AND
            datahora_fim_atendimento < CURRENT_DATETIME()
    ),

    api_agrupados_por_dia_e_cnes as (
        SELECT
            id_cnes,
            DATE(updated_at) as data,
            COUNT(*) as qtd_atendimentos
        FROM atendimentos_api
        GROUP BY 1, 2
    ),
    historico_agrupados_por_dia_e_cnes as (
        SELECT
            id_cnes,
            DATE(updated_at) as data,
            COUNT(*) as qtd_atendimentos
        FROM atendimentos_historico
        GROUP BY 1, 2
    ),

    juncao as (
        SELECT
            todas_as_combinacoes_de_dia_e_cnes.nome_limpo,
            todas_as_combinacoes_de_dia_e_cnes.area_programatica,
            todas_as_combinacoes_de_dia_e_cnes.id_cnes,
            todas_as_combinacoes_de_dia_e_cnes.data,

            COALESCE(api.qtd_atendimentos, 0) as qtd_atendimentos_api,
            COALESCE(historico.qtd_atendimentos, 0) as qtd_atendimentos_historico
        FROM todas_as_combinacoes_de_dia_e_cnes
            LEFT JOIN api_agrupados_por_dia_e_cnes api using (id_cnes, data)
            LEFT JOIN historico_agrupados_por_dia_e_cnes historico using (id_cnes, data)
    )

select * 
from juncao
order by 1,2,3,4