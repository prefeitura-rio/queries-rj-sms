{{
    config(
        alias="paciente_historico",
        materialized="table",
    )
}}
-- dbt run --select raw_prontuario_vitacare__paciente_historico 

WITH tb AS (
    SELECT 
        -- PK
        CAST(NULL AS STRING) AS id,

        -- Outras Chaves
        {{remove_accents_upper('N_CPF')}} AS cpf,
        {{remove_accents_upper('N_DNV')}} AS dnv,
        {{remove_accents_upper('NIS')}} AS nis,
        {{remove_accents_upper('N_CNS_DA_PESSOA_CADASTRADA')}} AS cns,
        cast(null as string) as id_local,

        -- Informações Pessoais
        {{remove_accents_upper('NOME_DA_PESSOA_CADASTRADA')}} AS nome,
        {{remove_accents_upper('NOME_SOCIAL_DA_PESSOA_CADASTRADA')}} AS nome_social,
        {{remove_accents_upper('NOME_DA_MAE_PESSOA_CADASTRADA')}} AS nome_mae,
        CAST(NULL AS STRING) AS nome_pai,
        {{remove_accents_upper('OBITO')}} AS obito,
        {{remove_accents_upper('OBITO')}} AS data_obito,
        {{remove_accents_upper('SEXO')}} AS sexo,
        {{remove_accents_upper('ORIENTACAO_SEXUAL')}} AS orientacao_sexual,
        {{remove_accents_upper('IDENTIDADE_GENERO')}} AS identidade_genero,
        {{remove_accents_upper('RACA_COR')}} AS raca_cor,

        -- Contato
        {{remove_accents_upper('EMAIL_CONTATO')}} AS email,
        {{remove_accents_upper('TELEFONE_CONTATO')}} AS telefone,

        -- Nascimento
        {{remove_accents_upper('NACIONALIDADE')}} AS nacionalidade,
        {{remove_accents_upper('DATA_DE_NASCIMENTO')}} AS data_nascimento,
        {{remove_accents_upper('PAIS_DE_NASCIMENTO')}} AS pais_nascimento,
        {{remove_accents_upper('MUNICIPIO_DE_NASCIMENTO')}} AS municipio_nascimento,
        CAST(NULL AS STRING) AS estado_nascimento,

        -- Informações da Unidade
        {{remove_accents_upper('AP')}} AS ap,
        {{remove_accents_upper('CODIGO_MICROAREA')}} AS microarea,
        {{remove_accents_upper('NUMERO_CNES_UNIDADE')}} AS cnes_unidade,
        CAST(NULL AS STRING) AS nome_unidade,
        {{remove_accents_upper('CODIGO_DA_EQUIPE_DE_SAUDE')}} AS codigo_equipe_saude,
        {{remove_accents_upper('CODIGO_INE_EQUIPE_DE_SAUDE')}} AS codigo_ine_equipe_saude,
        CAST(NULL AS TIMESTAMP) AS data_atualizacao_vinculo_equipe,
        {{remove_accents_upper('N_DO_PRONTUARIO')}} AS numero_prontuario,
        {{remove_accents_upper('N_DA_FAMILIA')}} AS numero_familia,
        CAST(NULL AS STRING) AS cadastro_permanente,
        {{remove_accents_upper('SITUACAO_USUARIO')}} AS situacao_usuario,
        {{remove_accents_upper('DATA_CADASTRO')}} AS data_cadastro_inicial,
        {{remove_accents_upper('DATA_ULTIMA_ATUALIZACAO_DO_CADASTRO')}} AS data_ultima_atualizacao_cadastral,

        -- Endereço
        {{remove_accents_upper('TIPO_DE_DOMICILIO')}} AS endereco_tipo_domicilio,
        {{remove_accents_upper('TIPO_DE_LOGRADOURO')}} AS endereco_tipo_logradouro,
        {{remove_accents_upper('CEP_LOGRADOURO')}} AS endereco_cep,
        {{remove_accents_upper('LOGRADOURO')}} AS endereco_logradouro,
        {{remove_accents_upper('BAIRRO_DE_MORADIA')}} AS endereco_bairro,
        CAST(NULL AS STRING) AS endereco_estado,
        CAST(NULL AS STRING) AS endereco_municipio,

        -- Metadata columns
        CAST(NULL AS DATE) AS data_particao,
        {{remove_accents_upper('updated_at')}} AS updated_at,
        {{remove_accents_upper('imported_at')}} AS imported_at
    FROM {{ source("brutos_prontuario_vitacare_staging", "paciente_historico_eventos") }}
),

padronized AS (
  SELECT 
    CAST(NULL AS STRING) AS id,

    -- OUTRAS CHAVES
    CASE 
      WHEN cpf IN ("NONE") THEN NULL
      WHEN REGEXP_CONTAINS(cpf, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$') THEN NULL
      ELSE CAST(cpf AS STRING)
    END AS cpf,
    CASE 
      WHEN dnv IN ("NONE") THEN NULL
      WHEN REGEXP_CONTAINS(dnv, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$') THEN NULL
      ELSE CAST(dnv AS STRING)
    END AS dnv,
    CASE 
      WHEN nis IN ("NONE") THEN NULL
      WHEN REGEXP_CONTAINS(nis, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$') THEN NULL
      ELSE CAST(nis AS STRING)
    END AS nis,
    CASE 
      WHEN cns IN ("NONE") THEN NULL
      WHEN REGEXP_CONTAINS(cns, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$') THEN NULL
      ELSE CAST(cns AS STRING)
    END AS cns,
    cast(null as string) as id_local,

    -- INFORMAÇÕES PESSOAIS
    CASE 
      {{remove_invalid_names('nome')}}
      ELSE CAST(nome AS STRING)
    END AS nome,
    CASE 
      {{remove_invalid_names('nome_social')}}
      ELSE CAST(nome_social AS STRING)
    END AS nome_social,
    CASE 
      {{remove_invalid_names('nome_mae')}}
      ELSE CAST(nome_mae AS STRING)
    END AS nome_mae,
    CASE 
      {{remove_invalid_names('nome_pai')}}
      ELSE CAST(nome_pai AS STRING)
    END AS nome_pai,
    CASE
        WHEN obito IN ("NONE") THEN 'False'
        ELSE 'True'
    END AS obito,
    CASE
        WHEN data_obito IN ("NONE") THEN NULL
        ELSE CAST(data_obito AS DATE FORMAT 'DD/MM/YYYY')
    END AS data_obito,
    CASE
      WHEN sexo IN ("NONE") THEN NULL
      WHEN sexo IN ("M") THEN CAST("male" AS STRING)
      WHEN sexo IN ("F") THEN CAST("female" AS STRING)
      ELSE NULL
    END AS sexo,
    CASE
        WHEN orientacao_sexual IN ("NONE") THEN NULL
        ELSE orientacao_sexual
    END AS orientacao_sexual,
    CASE
        WHEN identidade_genero IN ("NONE") THEN NULL
        WHEN identidade_genero IN ("HETEROSSEXUAL", "HETEROSSEXUAL", "HOMOSSEXUAL (GAY / LESBICA)", "BISSEXUAL") THEN INITCAP("CIS")
        WHEN identidade_genero IN ("MULHER TRANSEXUAL") THEN INITCAP("MULHER TRANSEXUAL")
        WHEN identidade_genero IN ("HOMEM TRANSEXUAL") THEN INITCAP("HOMEM TRANSEXUAL")
        WHEN identidade_genero IN ("OUTRO") THEN INITCAP("OUTRO")
        ELSE NULL
    END AS identidade_genero,
    CASE
        {{remove_invalid_names('raca_cor')}}
        WHEN raca_cor IN ("INDIGENA") THEN CAST(INITCAP("INDÍGENA") AS STRING)
        ELSE CAST(INITCAP(raca_cor) AS STRING)
    END AS raca_cor,

    -- CONTATO
    CASE
        {{remove_invalid_email('email')}}
        ELSE CAST(email AS STRING)
    END AS email,
    CASE
        WHEN telefone IN ("NONE") THEN NULL
        WHEN REGEXP_CONTAINS(telefone, r'^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$') THEN NULL
        WHEN REGEXP_CONTAINS(telefone, r'^\b21\b$') THEN NULL
        ELSE CAST(telefone AS STRING)
    END AS telefone,

    -- NASCIMENTO
    CASE
        WHEN nacionalidade IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(nacionalidade) AS STRING)
    END AS nacionalidade,
    CASE
        WHEN data_nascimento IN ("NONE") THEN NULL
        ELSE CAST(data_nascimento AS DATE FORMAT 'DD/MM/YYYY')
    END AS data_nascimento,
    CASE
        WHEN pais_nascimento IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(pais_nascimento) AS STRING)
    END AS pais_nascimento,
    CASE
        WHEN municipio_nascimento IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(municipio_nascimento) AS STRING)
    END AS municipio_nascimento, -- No rotineiro é o codigo IBGE, aqui esta sendo o nome
    CASE
        WHEN estado_nascimento IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(estado_nascimento) AS STRING)
    END AS estado_nascimento, 


    -- INFORMAÇÕES DA UNIDADE
    CASE
        WHEN ap IN ("NONE") THEN NULL
        ELSE CAST(ap AS STRING)
    END AS ap, 
    CASE
        WHEN microarea IN ("NONE") THEN NULL
        ELSE CAST(microarea AS STRING)
    END AS microarea,
    CASE
        WHEN cnes_unidade IN ("NONE") THEN NULL
        ELSE CAST(cnes_unidade AS STRING)
    END AS cnes_unidade,
    CASE
        WHEN nome_unidade IN ("NONE") THEN NULL
        ELSE CAST(nome_unidade AS STRING)
    END AS nome_unidade,
    CASE
        WHEN codigo_equipe_saude IN ("NONE") THEN NULL
        ELSE CAST(codigo_equipe_saude AS STRING)
    END AS codigo_equipe_saude,
    CASE
        WHEN codigo_ine_equipe_saude IN ("NONE") THEN NULL
        ELSE CAST(codigo_ine_equipe_saude AS STRING)
    END AS codigo_ine_equipe_saude,
    data_atualizacao_vinculo_equipe,
    CASE
        WHEN numero_prontuario IN ("NONE") THEN NULL
        ELSE CAST(numero_prontuario AS STRING)
    END AS numero_prontuario,
    CASE
        WHEN numero_familia IN ("NONE") THEN NULL
        ELSE CAST(numero_familia AS STRING)
    END AS numero_familia,
    CASE
        WHEN cadastro_permanente IN ("NONE") THEN NULL
        ELSE CAST(cadastro_permanente AS STRING)
    END AS cadastro_permanente,
    CASE
        WHEN situacao_usuario IN ("NONE") THEN NULL
        ELSE CAST(situacao_usuario AS STRING)
    END AS situacao_usuario,
    CASE
        WHEN data_cadastro_inicial IN ("NONE") THEN NULL
        ELSE CAST(data_cadastro_inicial AS TIMESTAMP FORMAT "DD/MM/YYYY")
    END AS data_cadastro_inicial,
    CASE
        WHEN data_ultima_atualizacao_cadastral IN ("NONE") THEN NULL
        ELSE CAST(data_ultima_atualizacao_cadastral AS TIMESTAMP FORMAT "DD/MM/YYYY")
    END AS data_ultima_atualizacao_cadastral,


    -- ENDEREÇO
    CASE
        WHEN endereco_tipo_domicilio IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(endereco_tipo_domicilio) AS STRING)
    END AS endereco_tipo_domicilio,
    CASE
        WHEN endereco_tipo_logradouro IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(endereco_tipo_logradouro) AS STRING)
    END AS endereco_tipo_logradouro,
    CASE
        WHEN endereco_cep IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(endereco_cep) AS STRING)
    END AS endereco_cep,

    CASE
        WHEN endereco_logradouro IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(endereco_logradouro) AS STRING)
    END AS endereco_logradouro,
    CASE
        WHEN endereco_bairro IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(endereco_bairro) AS STRING)
    END AS endereco_bairro,
    CASE
        WHEN endereco_estado IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(endereco_estado) AS STRING)
    END AS endereco_estado,
    CASE
        WHEN endereco_municipio IN ("NONE") THEN NULL
        ELSE CAST(INITCAP(endereco_municipio) AS STRING)
    END AS endereco_municipio,


    -- METADATA COLUMNS
    data_particao,
    CASE
        WHEN updated_at IN ("NONE") THEN NULL
        ELSE CAST(updated_at AS TIMESTAMP)
    END AS updated_at,
    CASE
        WHEN imported_at IN ("NONE") THEN NULL
        ELSE CAST(imported_at AS TIMESTAMP)
    END AS imported_at
  FROM tb
)

-- testing AS (
--   SELECT 
--     col,
--     count(*) AS count
--   FROM padronized
--   GROUP BY col
-- )

SELECT 
  *
FROM padronized