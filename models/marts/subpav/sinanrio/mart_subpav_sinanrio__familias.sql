{{ config(
    materialized = 'table',
    alias = 'familias_contatos',
    partition_by = {
        "field": "family_bucket",
        "data_type": "int64",
        "range": {"start": 0, "end": 2048, "interval": 1}
    },
    cluster_by = ["cpf_int","family_key_fp"]
) }}

WITH pacientes AS (
    SELECT
        CAST(numero_prontuario AS STRING)                                   AS prontuario_original,
        REGEXP_REPLACE(CAST(numero_prontuario AS STRING), r'\D','')         AS nump,
        LENGTH(REGEXP_REPLACE(CAST(numero_prontuario AS STRING), r'\D','')) AS len_nump,
        CAST(id_cnes AS STRING)                                             AS id_cnes,
        CAST(id_ine  AS STRING)                                             AS id_ine,
        SAFE_CAST(REGEXP_REPLACE(CAST(cpf AS STRING), r'\D','') AS INT64)   AS cpf_int,
        CAST(cpf AS STRING)                                                 AS cpf,
        CAST(cns AS STRING)                                                 AS cns,
        nome,
        SAFE_CAST(data_nascimento AS DATE)                                  AS data_nascimento,
        sexo                                                                AS sexo_original,
        raca                                                                AS raca_original,
        telefone,
        endereco_cep,
        endereco_tipo_logradouro,
        endereco_logradouro,
        endereco_numero,
        endereco_complemento,
        endereco_bairro,
        (obito_indicador IS FALSE OR obito_indicador IS NULL)               AS vivo,
        COALESCE(
        SAFE_CAST(source_updated_at                 AS TIMESTAMP),
        SAFE_CAST(data_ultima_atualizacao_cadastral AS TIMESTAMP),
        SAFE_CAST(data_atualizacao_vinculo_equipe   AS TIMESTAMP)
        ) AS recency_ts
    FROM {{ ref('raw_prontuario_vitacare__paciente') }}
    ),

    pre AS (
    SELECT
        *,
        {{ sinanrio_padronize_sexo('sexo_original') }}       AS sexo_id,
        {{ sinanrio_padronize_raca_cor('raca_original') }}   AS raca_cor_id,
        {{ remove_accents_upper('endereco_bairro') }}        AS bairro_norm
    FROM pacientes
    WHERE vivo
        AND nump    IS NOT NULL AND nump    <> ''
        AND id_cnes IS NOT NULL AND id_cnes <> ''
        AND cpf_int IS NOT NULL
        AND len_nump IN (13,14)
    ),

    dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
        SELECT p.*,
            ROW_NUMBER() OVER (PARTITION BY p.cpf_int ORDER BY p.recency_ts DESC) AS rn
        FROM pre p
    )
    WHERE rn = 1
    ),

    bairros AS (
    SELECT
        SAFE_CAST(id AS INT64)                  AS id_bairro,
        descricao                               AS nome_bairro,
        {{ remove_accents_upper('descricao') }} AS bairro_norm
    FROM {{ ref('raw_plataforma_subpav_principal__bairros') }}
    )

SELECT
    CONCAT(SUBSTR(d.nump, 1, d.len_nump - 2), ':', d.id_cnes) AS family_key,
    MOD(ABS(FARM_FINGERPRINT(CONCAT(SUBSTR(d.nump, 1, d.len_nump - 2), ':', d.id_cnes))), 2048) AS family_bucket,
    FARM_FINGERPRINT(CONCAT(SUBSTR(d.nump, 1, d.len_nump - 2), ':', d.id_cnes)) AS family_key_fp,
    d.cpf_int,
    d.id_cnes           AS cnes,
    d.id_ine            AS ine,
    d.cpf               AS cpf,
    d.cns               AS cns,
    d.nome              AS nome,
    d.data_nascimento   AS dt_nascimento,
    d.sexo_id           AS id_sexo,
    d.raca_cor_id       AS id_raca_cor,
    d.telefone          AS telefone,
    d.endereco_cep      AS cep,
    CONCAT(d.endereco_tipo_logradouro, ' ', d.endereco_logradouro) AS logradouro,
    d.endereco_numero   AS numero,
    d.endereco_complemento AS complemento,
    b.id_bairro         AS id_bairro
FROM dedup d
LEFT JOIN bairros b
ON d.bairro_norm = b.bairro_norm
