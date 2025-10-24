{{
    config(
        alias='vtc_completude_cpf',
        materialized='table',
    )
}}

WITH

    cadastros_bkp as (
        SELECT
            ut_id,
            cnes,
            case when {{ validate_cpf("cpf") }} then 1 else 0 end as indicador_cpf_valido
        FROM {{ source("brutos_prontuario_vitacare_historico_staging", "cadastro") }}
    ),
    cadastros_dedup as (
        SELECT distinct
            ut_id,
            cnes,
            indicador_cpf_valido,
        FROM cadastros_bkp
    ),
    cadastros as (
        SELECT
            cnes,
            count(*) as quantidade_cadastros,
            sum(indicador_cpf_valido) as quantidade_cadastros_com_cpf_valido,
        FROM cadastros_dedup
        GROUP BY 1
    ),
    enriquecimento as (
        SELECT
            cnes,
            est.nome_limpo,
            est.area_programatica,
            quantidade_cadastros,
            quantidade_cadastros_com_cpf_valido,
            round(quantidade_cadastros_com_cpf_valido / quantidade_cadastros * 100, 2) as completude_cpf,
        FROM cadastros
            LEFT JOIN {{ ref("dim_estabelecimento") }} est ON cadastros.cnes = est.id_cnes
        WHERE est.prontuario_versao = 'vitacare'
            AND est.prontuario_episodio_tem_dado = 'sim'
    )

select *
from enriquecimento