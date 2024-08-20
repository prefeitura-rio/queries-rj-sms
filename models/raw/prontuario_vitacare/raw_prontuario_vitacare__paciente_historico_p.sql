WITH tb AS (
    SELECT 
        -- PK
        CAST(NULL AS STRING) AS id,

        -- Outras Chaves

        {{remove_accents_upper('N_CPF')}} AS cpf,
        {{remove_accents_upper('N_DNV')}} AS dnv,
        {{remove_accents_upper('NIS')}} AS nis,
        {{remove_accents_upper('N_CNS_DA_PESSOA_CADASTRADA')}} AS cns,

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
    FROM `rj-sms`.`brutos_prontuario_vitacare_staging`.`paciente_historico_eventos`
),

padronized AS (
  SELECT 
    CAST(NULL AS STRING) AS id,
    -- Outras Chaves
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
    END AS col,
    CASE 
      {{remove_invalid_names('nome_pai')}}
      ELSE CAST(nome_pai AS STRING)
    END AS nome_pai
  FROM tb
),

testing AS (
  SELECT 
    col,
    count(*) AS count
  FROM padronized
  GROUP BY col
)

SELECT 
  *
FROM testing
ORDER BY count desc