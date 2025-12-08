{{ config(
   schema = "projeto_cdi",
   alias  = "pgm_oficios_mes",
   materialized = "table"
) }}

WITH base AS (
    SELECT
        processorio,
        data_de_entrada,
        situacao,
        cap,
        origem,
        sintese_solicitacao,
        mandado_de_prisao,
        crime_de_desobediencia,
        setor_responsavel

    FROM {{ ref('int_cdi__pgm') }}
    WHERE data_de_entrada IS NOT NULL
)

SELECT
    data_de_entrada,
    COUNT(DISTINCT processorio) AS total_oficios,

    COALESCE(situacao, 'Não informado')                AS situacao,
    COALESCE(cap, 'Não informado')                     AS cap,
    COALESCE(origem, 'Não informado')                  AS origem,
    COALESCE(sintese_solicitacao, 'Não informado')     AS sintese_solicitacao,
    COALESCE(mandado_de_prisao, 'Não informado')       AS mandado_de_prisao,
    COALESCE(crime_de_desobediencia, 'Não informado')  AS crime_de_desobediencia,
    COALESCE(setor_responsavel, 'Não informado')       AS setor_responsavel

FROM base
GROUP BY
    data_de_entrada,
    situacao, cap, origem, sintese_solicitacao,
    mandado_de_prisao, crime_de_desobediencia,
    setor_responsavel

ORDER BY data_de_entrada