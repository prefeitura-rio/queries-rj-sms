{{ config(
   schema = "projeto_cdi",
   alias  = "pgm_oficios_mes",
   materialized = "table",
   meta={"owner": "karen"}
) }}

WITH base AS (
    SELECT
        processo_rio,
        data_entrada,
        situacao,
        cap,
        origem,
        sintese_solicitacao,
        mandado_prisao,
        crime_desobediencia,
        setor_responsavel

    FROM {{ ref('int_cdi__pgm') }}
    WHERE data_entrada IS NOT NULL
)

SELECT
    data_entrada,
    COUNT(DISTINCT processo_rio) AS total_oficios,

    COALESCE(situacao, 'Não informado')               AS situacao,
    COALESCE(cap, 'Não informado')                    AS cap,
    COALESCE(origem, 'Não informado')                 AS origem,
    COALESCE(sintese_solicitacao, 'Não informado')    AS sintese_solicitacao,
    COALESCE(mandado_prisao, 'Não informado')         AS mandado_prisao,
    COALESCE(crime_desobediencia, 'Não informado')    AS crime_desobediencia,
    COALESCE(setor_responsavel, 'Não informado')      AS setor_responsavel

FROM base
GROUP BY
    data_entrada,
    situacao, cap, origem, sintese_solicitacao,
    mandado_prisao, crime_desobediencia,
    setor_responsavel

ORDER BY data_entrada