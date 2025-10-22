{{
    config(
        alias='vtc_atraso_atendimentos',
        materialized='table',
    )
}}

WITH

    atendimentos_api_continuo as (
        SELECT
            payload_cnes as id_cnes,
            source_id,
            TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") as momento_ocorrencia,
            datalake_loaded_at as momento_ingestao,
        FROM {{ source("brutos_prontuario_vitacare_api_staging", "atendimento_continuo") }}
        WHERE
            TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") >= DATETIME_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AND
            TIMESTAMP(DATETIME(source_updated_at), "America/Sao_Paulo") <= DATETIME_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    ),

    ocorrencias as (
        select *, concat('atendimento', '.', source_id) as id_ocorrencia
        from atendimentos_api_continuo
    ),

    calculo_atraso as (
        SELECT
            id_cnes,
            id_ocorrencia,
            momento_ocorrencia,
            momento_ingestao,
            TIMESTAMP_DIFF(momento_ingestao, momento_ocorrencia, MINUTE) as atraso_de_ingestao,
        FROM ocorrencias
    ),

    removendo_duplicados as (
        SELECT
            id_cnes,
            id_ocorrencia,
            MIN(momento_ocorrencia) as momento_ocorrencia,
            MIN(momento_ingestao) as momento_ingestao,
            MIN(atraso_de_ingestao) as atraso_de_ingestao,
        FROM calculo_atraso
        GROUP BY 1, 2
    )

select *
from removendo_duplicados
order by rand()
limit 100000