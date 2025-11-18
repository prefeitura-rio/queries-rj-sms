{{ 
    config(
        materialized='table',
        alias = "resultado_exame",
    ) 
}}

WITH base AS (
    SELECT
        NULLIF(REGEXP_REPLACE(
        CASE
            WHEN REGEXP_CONTAINS(UPPER(tipo_doc_paciente_1), r'CPF') THEN documento_paciente_1
            WHEN REGEXP_CONTAINS(UPPER(tipo_doc_paciente_2), r'CPF') THEN documento_paciente_2
            ELSE NULL
        END, r'\D', ''), ''
        ) AS paciente_cpf,

        REGEXP_REPLACE(cns_paciente, r'\D', '') AS paciente_cns,
        UPPER(paciente) as nome,
        NULLIF(TRIM(codigo_amostra), '') AS codigo_amostra,

        COALESCE(
        NULLIF(TRIM(cnes_unidade_solicitante), ''),
        NULLIF(TRIM(cnes_unidade_notificacao_sinan), ''),
        NULLIF(TRIM(cnes_laboratorio_execucao), ''),
        NULLIF(TRIM(cnes_laboratorio_responsavel), ''),
        NULLIF(TRIM(cnes_laboratorio_cadastro), '')
        ) AS cnes,

        CASE
        WHEN tipo_exame = 'tugexp-pcrtr' THEN 1
        WHEN tipo_exame = 'tubb-colzn'  THEN 2
        END AS id_tipo_exame,

        COALESCE(
        COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_liberacao),         SAFE.PARSE_DATE('%Y-%m-%d', data_liberacao),         SAFE.PARSE_DATE('%d-%m-%Y', data_liberacao)),
        COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_processamento),     SAFE.PARSE_DATE('%Y-%m-%d', data_processamento),     SAFE.PARSE_DATE('%d-%m-%Y', data_processamento)),
        COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_inicio_processamento), SAFE.PARSE_DATE('%Y-%m-%d', data_inicio_processamento), SAFE.PARSE_DATE('%d-%m-%Y', data_inicio_processamento)),
        COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_recebimento),       SAFE.PARSE_DATE('%Y-%m-%d', data_recebimento),       SAFE.PARSE_DATE('%d-%m-%Y', data_recebimento)),
        COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_solicitacao),       SAFE.PARSE_DATE('%Y-%m-%d', data_solicitacao),       SAFE.PARSE_DATE('%d-%m-%Y', data_solicitacao))
        ) AS dt_resultado,

        UPPER(tugexp_pcrtr.dna_para_complexo_mycobacterium_tuberculosis) AS pcr_dna,
        UPPER(tugexp_pcrtr.rifampicina)                                  AS pcr_rif,
        UPPER(tubb_colzn.resultado)                                      AS ziehl_res,

        loaded_at
    FROM {{ ref('raw_gal__exames_laboratoriais') }}
    WHERE tipo_exame IN ('tugexp-pcrtr','tubb-colzn')
),
classificado AS (
    SELECT
        codigo_amostra,
        paciente_cpf,
        paciente_cns,
        nome,
        cnes,
        id_tipo_exame,
        dt_resultado,
        CASE
        WHEN id_tipo_exame = 1 THEN
            CASE
            WHEN REGEXP_CONTAINS(pcr_dna, r'N[ÃA]O\s*TESTA') THEN 5
            WHEN REGEXP_CONTAINS(pcr_dna, r'N[ÃA]O\s*DETEC') THEN 3
            WHEN REGEXP_CONTAINS(pcr_dna, r'INCONCLUS|INDETERMIN') THEN 4
            WHEN REGEXP_CONTAINS(pcr_dna, r'DETEC') AND REGEXP_CONTAINS(pcr_dna, r'TRA[ÇC]') THEN 4
            WHEN REGEXP_CONTAINS(pcr_dna, r'DETEC') THEN
                CASE
                WHEN REGEXP_CONTAINS(pcr_rif, r'RESIST') THEN 2
                WHEN pcr_rif IS NULL OR TRIM(pcr_rif) = '' OR REGEXP_CONTAINS(pcr_rif, r'SENSI|SUSCET|INDETERMIN') THEN 1
                ELSE 1
                END
            ELSE NULL
            END
        WHEN id_tipo_exame = 2 THEN
            CASE
                -- precedência: +++ > ++ > + (exatamente um)
                WHEN REGEXP_CONTAINS(ziehl_res, r'\+{3}') THEN 1
                WHEN REGEXP_CONTAINS(ziehl_res, r'\+{2}') THEN 5
                WHEN REGEXP_CONTAINS(ziehl_res, r'\+') AND NOT REGEXP_CONTAINS(ziehl_res, r'\+{2,}') THEN 4

                -- "Encontrado(s) N B.A.A.R." (N >= 1)
                WHEN REGEXP_CONTAINS(ziehl_res, r'ENCONTRAD[OA]S?\s+\b[1-9][0-9]*\b\s+B\.A\.A\.R') THEN 4

                -- "Positiva ..." sem "+" explícito
                WHEN REGEXP_CONTAINS(ziehl_res, r'\bPOSITIV') AND NOT REGEXP_CONTAINS(ziehl_res, r'\+') THEN 4

                -- negativos (variações vistas no GAL)
                WHEN REGEXP_CONTAINS(ziehl_res, r'AUS[ÊE]NCIA\s+DE\s+B\.A\.A\.R')
                    OR REGEXP_CONTAINS(ziehl_res, r'ENCONTRAD[OA]S?\s+\b0\b\s+B\.A\.A\.R')
                    OR REGEXP_CONTAINS(ziehl_res, r'\bNEGATIV') THEN 2

                -- presença textual de BAAR sem graduação
                WHEN REGEXP_CONTAINS(ziehl_res, r'PRESEN[ÇC]A.*B\.A\.A\.R') THEN 4
                ELSE NULL
            END
        END AS id_resultado,
        loaded_at
    FROM base
),
dedup AS (
    SELECT
        codigo_amostra,
        paciente_cpf,
        paciente_cns,
        nome,
        cnes,
        id_tipo_exame,
        id_resultado,
        dt_resultado,
        ROW_NUMBER() OVER (
            PARTITION BY paciente_cns, id_tipo_exame, dt_resultado
            ORDER BY loaded_at DESC
        ) AS rn
    FROM classificado
)
SELECT
    codigo_amostra,
    paciente_cpf,
    paciente_cns,
    nome,
    cnes,
    id_tipo_exame,
    id_resultado,
    dt_resultado,
    diagnostico -- Verificar se é diagnostico ou acompanhamento (1 = diagnostco e 0 não)
FROM dedup
WHERE dt_resultado IS NOT NULL
    AND id_tipo_exame IS NOT NULL
    AND id_resultado IS NOT NULL
    AND REGEXP_CONTAINS(paciente_cns, r'^\d{15}$')
    AND paciente_cns <> '000000000000000'
    AND rn = 1
