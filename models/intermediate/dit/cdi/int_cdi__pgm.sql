{{ 
    config(
        schema = "intermediario_cdi",
        alias  = "pgm",
        materialized = "table",
        meta={"owner": "karen"}
) }}

WITH base AS (

    SELECT
        processorio,
        procuradora,
        requerente,
        processo_judicial,
        origem,
        data_de_entrada,
        data_de_saida,
        data_saida_pgm,
        prazo,
        mes_ano,
        sexo,
        idade,
        hospital_de_origem,
        cap,
        erro_medico,
        acp,
        tipo_indenizacao,
        valor,
        mandado_de_prisao,
        crime_de_desobediencia,
        patologia_assunto,
        solicitacao,
        sintese_solicitacao,
        setor_responsavel,
        prazo_dias,
        situacao,
        pendencias,
        observacoes
    FROM {{ ref('raw_cdi__pgm_2025') }}

    UNION ALL

    SELECT
        processorio,
        procuradora,
        requerente,
        processo_judicial,
        origem,
        data_de_entrada,
        data_de_saida,
        data_saida_pgm,
        prazo,
        mes_ano,
        sexo,
        idade,
        hospital_de_origem,
        cap,
        erro_medico,
        acp,
        tipo_indenizacao,
        valor,
        mandado_de_prisao,
        crime_de_desobediencia,
        patologia_assunto,
        solicitacao,
        sintese_solicitacao,
        setor_responsavel,
        prazo_dias,
        situacao,
        pendencias,
        observacoes
    FROM {{ ref('raw_cdi__pgm_2026') }}

)

SELECT *
FROM base
WHERE NOT (
    data_de_entrada IS NULL
    AND processo_judicial IS NULL
    AND processorio IS NULL
)