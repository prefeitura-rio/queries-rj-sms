{{ config(
    schema = "projeto_cdi",
    alias  = "jr_perfil_solicitacoes",
    materialized = "table"
) }}

WITH base AS (
    SELECT
        SAFE_CAST(processo_rio AS STRING) AS processo_rio,
        INITCAP(TRIM(solicitacao)) AS tipo_solicitacao,
        INITCAP(TRIM(orgao_para_subsidiar)) AS orgao_para_subsidiar,

        LOWER(TRIM(idade)) AS idade_classe,
        UPPER(TRIM(sexo)) AS sexo_raw,

        SAFE_CAST(DATE(data) AS DATE) AS data_solicitacao,

        COALESCE(
            INITCAP(NULLIF(TRIM(situacao), '')),
            'Não informado'
        ) AS situacao
    FROM {{ ref('int_cdi__judicial_residual') }}
    WHERE data IS NOT NULL
),

limpo AS (
    SELECT
        *,
        CASE 
            WHEN sexo_raw = 'F' THEN 'Feminino'
            WHEN sexo_raw = 'M' THEN 'Masculino'
            WHEN sexo_raw IN ('F/M', 'M/F') THEN 'Ambos'
            ELSE 'Não Informado'
        END AS sexo_norm,

        CASE
            WHEN REGEXP_CONTAINS(idade_classe, r'\badult') THEN 'Adulto'
            WHEN REGEXP_CONTAINS(idade_classe, r'\bidos') THEN 'Idoso'
            WHEN REGEXP_CONTAINS(idade_classe, r'crian[cç]a') THEN 'Criança'
            WHEN REGEXP_CONTAINS(idade_classe, r'adolesc') THEN 'Adolescente'
            WHEN REGEXP_CONTAINS(idade_classe, r'\brn\b|rec[eé]m[- ]?nascid') THEN 'Recém-nascido'
            WHEN REGEXP_CONTAINS(idade_classe, r'n[uú]cleo\s*familiar|fam[ií]li') THEN 'Núcleo familiar'
            WHEN idade_classe IS NULL THEN 'Não informado'
            ELSE INITCAP(idade_classe)
        END AS faixa_etaria_norm
    FROM base
)

SELECT
    processo_rio,
    tipo_solicitacao,
    orgao_para_subsidiar,
    sexo_norm AS sexo,
    faixa_etaria_norm AS faixa_etaria,
    data_solicitacao,
    situacao
FROM limpo
ORDER BY data_solicitacao