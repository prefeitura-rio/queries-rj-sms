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
    vacinacoes_historico as (
        SELECT
            case
                when vacina.data_aplicacao != '1900-01-01'
                    then vacina.data_aplicacao
                when vacina.data_aplicacao = '1900-01-01' and vacina.tipo_registro = 'Vaccine administration'
                    then cast(acto.datahora_fim_atendimento as date)
                else null
            end as date,
            vacina.id_cnes,
            id_vacinacao
        FROM {{ ref("raw_prontuario_vitacare_historico__vacina") }} vacina
            INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} acto using (id_prontuario_global)
    ),
    vacinacoes_api as (
        SELECT
            case
                when vacina.data_aplicacao != '1900-01-01'
                    then vacina.data_aplicacao
                when vacina.data_aplicacao = '1900-01-01' and vacina.tipo_registro = 'Vaccine administration'
                    then cast(acto.datahora_fim_atendimento as date)
                else null
            end as date,
            vacina.id_cnes,
            id_vacinacao
        FROM {{ ref("raw_prontuario_vitacare_api__vacina") }} vacina
            INNER JOIN {{ ref("raw_prontuario_vitacare_api__acto") }} acto using (id_prontuario_global)
    ),

    juncao_vacinacoes as (
        SELECT * FROM vacinacoes_historico
        UNION ALL
        SELECT * FROM vacinacoes_api
    ),

    deduplicacao_vacinacoes as (
        SELECT DISTINCT * FROM juncao_vacinacoes
    ),

    agrupamento_por_dia_e_cnes as (
        SELECT
            date,
            id_cnes,
            COUNT(*) as qtd_vacinacoes
        FROM deduplicacao_vacinacoes
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