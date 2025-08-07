{{ config(
    schema = "projeto_whatsapp",
    alias = "telefones_validos",
    materialized = "table"
) }}

WITH ddds_validos AS (
  SELECT ddd
  FROM UNNEST([
    '11','12','13','14','15','16','17','18','19',
    '21','22','24','27','28',
    '31','32','33','34','35','37','38',
    '41','42','43','44','45','46','47','48','49',
    '51','53','54','55',
    '61','62','63','64','65','66','67','68','69',
    '71','73','74','75','77','79',
    '81','82','83','84','85','86','87','88','89',
    '91','92','93','94','95','96','97','98','99'
  ]) AS ddd
),

base_telefones AS (
  SELECT
    TRIM(cpf) AS cpf,
    TRIM(telefone) AS telefone,
    data_ultima_atualizacao_cadastral,
    'vitacare' AS origem
  FROM `rj-sms.brutos_prontuario_vitacare.paciente`
  WHERE telefone IS NOT NULL AND telefone != ''

  UNION ALL

  SELECT
    TRIM(cpf),
    TRIM(celular),
    updated_at,
    'vitai'
  FROM `rj-sms.brutos_prontuario_vitai_api.paciente`
  WHERE celular IS NOT NULL AND celular != ''

  UNION ALL

  SELECT
    TRIM(cpf),
    TRIM(tel) AS telefone,
    updated_at,
    'vitai'
  FROM (
    SELECT cpf, updated_at, telefone_extra_um, telefone_extra_dois
    FROM `rj-sms.brutos_prontuario_vitai_api.paciente`
  ), UNNEST([telefone_extra_um, telefone_extra_dois]) AS tel
  WHERE tel IS NOT NULL AND tel != ''

  UNION ALL

  SELECT
    TRIM(cpf),
    TRIM(telefone),
    updated_at,
    'smsrio'
  FROM `rj-sms.brutos_plataforma_smsrio.paciente`
  WHERE telefone IS NOT NULL AND telefone != ''
),

limpa_telefone AS (
  SELECT
    *,
    CASE
      WHEN REGEXP_CONTAINS(REGEXP_REPLACE(telefone, r'[^0-9]', ''), r'^0\d{11}$')
        THEN SUBSTR(REGEXP_REPLACE(telefone, r'[^0-9]', ''), 2)
      ELSE REGEXP_REPLACE(telefone, r'[^0-9]', '')
    END AS telefone_limpo
  FROM base_telefones
),

formatacao AS (
  SELECT
    f.*,
    LENGTH(f.telefone_limpo) AS tamanho,
    IF((d.ddd IS NOT NULL) and (char_length(f.telefone_limpo)>9), true, false) AS ddd_valido,

    CASE
      WHEN LENGTH(f.telefone_limpo) = 11
           AND d.ddd IS NOT NULL
           AND SUBSTR(f.telefone_limpo, 3, 1) = '9'
           AND NOT (
             REPEAT(SUBSTR(f.telefone_limpo, 1, 1), LENGTH(f.telefone_limpo)) = f.telefone_limpo
             AND LENGTH(f.telefone_limpo) > 0
           )
        THEN CONCAT('55', f.telefone_limpo)

      WHEN LENGTH(f.telefone_limpo) = 10
           AND d.ddd IS NOT NULL
           AND SUBSTR(f.telefone_limpo, 3, 1) BETWEEN '6' AND '9'
           AND NOT (
             REPEAT(SUBSTR(f.telefone_limpo, 1, 1), LENGTH(f.telefone_limpo)) = f.telefone_limpo
             AND LENGTH(f.telefone_limpo) > 0
           )
        THEN CONCAT('55', SUBSTR(f.telefone_limpo, 1, 2), '9', SUBSTR(f.telefone_limpo, 3))

      WHEN LENGTH(f.telefone_limpo) = 13
           AND SUBSTR(f.telefone_limpo, 1, 2) = '55'
           AND EXISTS (
             SELECT 1 FROM ddds_validos dv
             WHERE dv.ddd = SUBSTR(f.telefone_limpo, 3, 2)
           )
           AND SUBSTR(f.telefone_limpo, 5, 1) = '9'
           AND NOT (
             REPEAT(SUBSTR(f.telefone_limpo, 1, 1), LENGTH(f.telefone_limpo)) = f.telefone_limpo
             AND LENGTH(f.telefone_limpo) > 0
           )
        THEN f.telefone_limpo

      ELSE NULL
    END AS telefone_formatado

  FROM limpa_telefone f
  LEFT JOIN ddds_validos d
    ON SUBSTR(f.telefone_limpo, 1, 2) = d.ddd
),

telefones_clinicas AS (
  SELECT DISTINCT
    CASE
      WHEN LENGTH(telefone_limpo) = 11
           AND SUBSTR(telefone_limpo, 3, 1) = '9'
        THEN CONCAT('55', telefone_limpo)

      WHEN LENGTH(telefone_limpo) = 10
           AND SUBSTR(telefone_limpo, 3, 1) BETWEEN '6' AND '9'
        THEN CONCAT('55', SUBSTR(telefone_limpo, 1, 2), '9', SUBSTR(telefone_limpo, 3))

      WHEN LENGTH(telefone_limpo) = 13
           AND SUBSTR(telefone_limpo, 1, 2) = '55'
           AND SUBSTR(telefone_limpo, 5, 1) = '9'
        THEN telefone_limpo

      ELSE NULL
    END AS telefone_clinica_formatado
  FROM (
    SELECT
      REGEXP_REPLACE(telefone_elemento, r'[^0-9]', '') AS telefone_limpo
    FROM `rj-sms-dev.saude_dados_mestres.estabelecimento`,
    UNNEST(telefone) AS telefone_elemento
  )
  WHERE telefone_limpo IS NOT NULL AND telefone_limpo != ''
),

frequencia AS (
  SELECT
    *,
    CASE
      WHEN telefone_limpo IS NOT NULL
        THEN COUNT(DISTINCT cpf) OVER (PARTITION BY telefone_limpo)
      ELSE NULL
    END AS total_cpfs_com_mesmo_telefone
  FROM formatacao
),

avaliacoes AS (
  SELECT
    *,
    telefone_formatado IS NULL AS flag_telefone_formatado_nulo,
    CASE 
      WHEN telefone_limpo IS NULL THEN FALSE
      ELSE total_cpfs_com_mesmo_telefone >= 10
    END AS flag_numero_compartilhado,
    UPPER(TRIM(telefone)) IN (
      'NAO INFORMADO', 'NAO TEM', 'NAO POSSUI', 'NONE', 'SEM INFORMACAO', 'SEM TELEFONE',
      'NAO TEM CELULAR', 'NAO INFORMOU', 'SEM INF', 'SEM TELEFONE NO MOMENTO', 'SEM CONTATO',
      'S/TEL', 'S/N', 'SN', '-', 'X', 'XX', 'XXX', 'XXXX', 'XXXXX', 'XXXXXX', 'XXXXXXX',
      'XXXXXXXX', 'XXXXXXXXX', 'XXXXXXXXXX', 'XXXXXXXXXXXX', '()'
    ) AS flag_texto_indefinido,
    LENGTH(telefone_limpo) < 8 AS flag_poucos_digitos,
    REPEAT(SUBSTR(telefone_limpo, 1, 1), LENGTH(telefone_limpo)) = telefone_limpo AND LENGTH(telefone_limpo) > 0 AS flag_todos_digitos_iguais,
    REGEXP_CONTAINS(telefone_limpo, r'0{5,}|1{5,}|2{5,}|3{5,}|4{5,}|5{5,}|6{5,}|7{5,}|8{5,}|9{5,}') AS flag_repetidos_5_ou_mais,
    NOT ddd_valido AS flag_ddd_invalido,
    (LENGTH(telefone_limpo) = 11 AND SUBSTR(telefone_limpo, 3, 1) != '9') AS flag_celular_9d_digito_invalido,
    (
      REGEXP_CONTAINS(telefone_limpo, r'^(0800|0300|0500|400[0-9])') OR
      telefone_limpo IN ('2134601746', '6133152425')
    ) AS flag_numero_institucional
  FROM frequencia
),

final AS (
  SELECT
    a.*,
    EXISTS (
      SELECT 1
      FROM telefones_clinicas tc
      WHERE tc.telefone_clinica_formatado = a.telefone_formatado
    ) AS flag_telefone_clinica,

    (
      a.flag_telefone_formatado_nulo OR
      a.flag_numero_compartilhado OR
      a.flag_texto_indefinido OR
      a.flag_poucos_digitos OR
      a.flag_todos_digitos_iguais OR
      a.flag_repetidos_5_ou_mais OR
      a.flag_ddd_invalido OR
      a.flag_celular_9d_digito_invalido OR
      a.flag_numero_institucional OR
      EXISTS (
        SELECT 1
        FROM telefones_clinicas tc
        WHERE tc.telefone_clinica_formatado = a.telefone_formatado
      )
    ) AS flag_numero_invalidado
  FROM avaliacoes a
)

SELECT
  cpf,
  ARRAY_AGG(STRUCT(
    telefone AS telefone_raw,
    telefone_limpo,
    if(flag_numero_invalidado is true, null, telefone_formatado) as telefone_formatado,
    data_ultima_atualizacao_cadastral,
    STRUCT(
      flag_numero_invalidado,
      flag_telefone_formatado_nulo,
      total_cpfs_com_mesmo_telefone,
      flag_numero_compartilhado,
      flag_texto_indefinido,
      flag_poucos_digitos,
      flag_todos_digitos_iguais,
      flag_repetidos_5_ou_mais,
      flag_ddd_invalido,
      flag_celular_9d_digito_invalido,
      flag_numero_institucional,
      flag_telefone_clinica
    ) AS status
  ) ORDER BY data_ultima_atualizacao_cadastral DESC) AS telefones
FROM final
WHERE cpf IS NOT NULL
  AND cpf != ''
  AND UPPER(cpf) NOT IN ('00000000000', 'NONE', 'NAO TEM', 'NAO INFORMADO')
GROUP BY cpf
