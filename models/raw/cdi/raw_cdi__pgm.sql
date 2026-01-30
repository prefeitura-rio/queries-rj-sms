{{ config(
    schema = "brutos_cdi",
    alias  = "pgm",
    materialized = "table"
) }}

WITH pgm_2025 AS (

  SELECT
    {{ normalize_null("regexp_replace(trim(processorio___sei), r'[\\n\\r]', '')") }} AS processorio,
    {{ normalize_null("regexp_replace(trim(procuradora), r'[\\n\\r\\t]+', '')") }} AS procuradora,
    {{ normalize_null("regexp_replace(trim(requerente), r'[\\n\\r\\t]+', '')") }} AS requerente,
    {{ normalize_null("regexp_replace(trim(processo_judicial), r'[\\n\\r]', '')") }} AS processo_judicial,
    {{ normalize_null("regexp_replace(trim(origem), r'[\\n\\r\\t]+', '')") }} AS origem,

    {{ cdi_parse_date('data_de_entrada', 'processorio___sei', 'processo_judicial') }} AS data_de_entrada,
    {{ cdi_parse_date('data_de_saida', 'processorio___sei', 'processo_judicial') }} AS data_de_saida,
    {{ cdi_parse_date('data_de_saida_para_pgm', 'processorio___sei', 'processo_judicial') }} AS data_de_saida_para_pgm,
    {{ cdi_parse_date('prazo', 'processorio___sei', 'processo_judicial') }} AS prazo,
    {{ cdi_parse_date('mes_ano', 'processorio___sei', 'processo_judicial') }} AS mes_ano,

    TRIM({{ normalize_null('sexo') }}) AS sexo,
    {{ normalize_null('idade') }} AS idade,

    UPPER(
      CASE
        WHEN REGEXP_CONTAINS(
          LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE({{ normalize_null('hospital_de_origem') }}, NFD), r'\pM', ''), r'\s+', ' '))),
          r'do+m'
        ) THEN 'Domicilio'
        ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE({{ normalize_null('hospital_de_origem') }}, NFD), r'\pM', ''), r'\s+', ' '))
      END
    ) AS hospital_de_origem,

    TRIM({{ normalize_null('cap') }}) AS cap,
    TRIM({{ normalize_null('erro_medico') }}) AS erro_medico,
    TRIM({{ normalize_null('acp') }}) AS acp,
    TRIM({{ normalize_null('multa_bloqueio_de_verba_indenizacao') }}) AS multa_bloqueio_de_verba_indenizacao,
    SAFE_CAST({{ normalize_null('valor') }} AS FLOAT64) AS valor,
    TRIM({{ normalize_null('mandado_de_prisao') }}) AS mandado_de_prisao,
    TRIM({{ normalize_null('crime_de_desobediencia') }}) AS crime_de_desobediencia,
    REGEXP_REPLACE(TRIM({{ normalize_null('patologia___assunto') }}), r'\s+', ' ') AS patologia_assunto,
    REGEXP_REPLACE(TRIM({{ normalize_null('solicitacao') }}), r'\s+', ' ') AS solicitacao,
    REGEXP_REPLACE(TRIM({{ normalize_null('sintese_de_solicitacao') }}), r'\s+', ' ') AS sintese_de_solicitacao,
    {{ normalize_null('setor_responsavel_pela_resposta') }} AS setor_responsavel_pela_resposta,
    SAFE_CAST({{ normalize_null('prazo_dias') }} AS INT64) AS prazo_dias,

    CASE
      WHEN LOWER(TRIM(CAST(situacao AS STRING))) IN ('#ref!', '#value!') THEN NULL
      ELSE {{ normalize_null('situacao') }}
    END AS situacao,

    TRIM({{ normalize_null('pendencias') }}) AS pendencias,
    REGEXP_REPLACE(TRIM({{ normalize_null('observacoes') }}), r'\s+', ' ') AS observacoes

  FROM {{ source("brutos_cdi_staging", "pgm") }}

),

pgm_2026 AS (

  SELECT
    *
  FROM pgm_2025

),

base_2026 AS (

  SELECT
    {{ normalize_null("regexp_replace(trim(processorio___sei), r'[\\n\\r]', '')") }} AS processorio,
    {{ normalize_null("regexp_replace(trim(procuradora), r'[\\n\\r\\t]+', '')") }} AS procuradora,
    {{ normalize_null("regexp_replace(trim(requerente), r'[\\n\\r\\t]+', '')") }} AS requerente,
    {{ normalize_null("regexp_replace(trim(processo_judicial), r'[\\n\\r]', '')") }} AS processo_judicial,
    {{ normalize_null("regexp_replace(trim(origem), r'[\\n\\r\\t]+', '')") }} AS origem,

    {{ cdi_parse_date('data_de_entrada', 'processorio___sei', 'processo_judicial') }} AS data_de_entrada,
    {{ cdi_parse_date('data_de_saida', 'processorio___sei', 'processo_judicial') }} AS data_de_saida,
    {{ cdi_parse_date('data_de_saida_para_pgm', 'processorio___sei', 'processo_judicial') }} AS data_de_saida_para_pgm,
    {{ cdi_parse_date('prazo', 'processorio___sei', 'processo_judicial') }} AS prazo,
    {{ cdi_parse_date('mes_ano', 'processorio___sei', 'processo_judicial') }} AS mes_ano,

    TRIM({{ normalize_null('sexo') }}) AS sexo,
    {{ normalize_null('idade') }} AS idade,

    UPPER(
      CASE
        WHEN REGEXP_CONTAINS(
          LOWER(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE({{ normalize_null('hospital_de_origem') }}, NFD), r'\pM', ''), r'\s+', ' '))),
          r'do+m'
        ) THEN 'Domicilio'
        ELSE TRIM(REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE({{ normalize_null('hospital_de_origem') }}, NFD), r'\pM', ''), r'\s+', ' '))
      END
    ) AS hospital_de_origem,

    TRIM({{ normalize_null('cap') }}) AS cap,
    TRIM({{ normalize_null('erro_medico') }}) AS erro_medico,
    TRIM({{ normalize_null('acp') }}) AS acp,
    TRIM({{ normalize_null('multa_bloqueio_de_verba_indenizacao') }}) AS multa_bloqueio_de_verba_indenizacao,
    SAFE_CAST({{ normalize_null('valor') }} AS FLOAT64) AS valor,
    TRIM({{ normalize_null('mandado_de_prisao') }}) AS mandado_de_prisao,
    TRIM({{ normalize_null('crime_de_desobediencia') }}) AS crime_de_desobediencia,
    REGEXP_REPLACE(TRIM({{ normalize_null('patologia___assunto') }}), r'\s+', ' ') AS patologia_assunto,
    REGEXP_REPLACE(TRIM({{ normalize_null('solicitacao') }}), r'\s+', ' ') AS solicitacao,
    REGEXP_REPLACE(TRIM({{ normalize_null('sintese_de_solicitacao') }}), r'\s+', ' ') AS sintese_de_solicitacao,
    {{ normalize_null('setor_responsavel_pela_resposta') }} AS setor_responsavel_pela_resposta,
    SAFE_CAST({{ normalize_null('prazo_dias') }} AS INT64) AS prazo_dias,

    CASE
      WHEN LOWER(TRIM(CAST(situacao AS STRING))) IN ('#ref!', '#value!') THEN NULL
      ELSE {{ normalize_null('situacao') }}
    END AS situacao,

    TRIM({{ normalize_null('pendencias') }}) AS pendencias,
    REGEXP_REPLACE(TRIM({{ normalize_null('observacoes') }}), r'\s+', ' ') AS observacoes

  FROM {{ source("brutos_cdi_staging", "pgm_2026") }}

)

SELECT * FROM pgm_2025
UNION ALL
SELECT * FROM base_2026
