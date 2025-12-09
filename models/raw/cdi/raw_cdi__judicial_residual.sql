{{ config(
    schema = "brutos_cdi",
    alias  = "judicial_residual",
    materialized = "table",
) }}

with base as (
  select

    {{ normalize_null("regexp_replace(trim(processo_rio), r'[\\n\\r]', '')") }} as processo_rio,
    {{ normalize_null("upper(regexp_replace(trim(mrj_e_parte), r'[\\n\\r\\t]+', ''))") }} as mrj_e_parte,
    {{ normalize_null('oficio') }}                                              as oficio,

    -- Formata datas que vem com erro de digitacao
    {{ cdi_parse_date('data','processo_rio','oficio') }}  AS data,
    {{ cdi_parse_date('entrada_gat_3','processo_rio','oficio') }} AS entrada_gat_3,
    {{ cdi_parse_date('vencimento','processo_rio','oficio') }} AS vencimento,
    {{ cdi_parse_date('retorno','processo_rio','oficio') }} AS retorno,
    {{ cdi_parse_date('data_de_saida','processo_rio','oficio') }} AS data_de_saida,
    {{ cdi_parse_date('data_do_oficio','processo_rio','oficio') }} AS data_do_oficio,
    {{ cdi_parse_date('pg_pas_dta_sfc','processo_rio','oficio') }} AS pg_pas_dta_sfc,

    {{ normalize_null('orgao') }}                                               as orgao,
    {{ normalize_null('processo') }}                                            as processo,
    {{ normalize_null('assunto') }}                                             as assunto,
    REGEXP_REPLACE(TRIM({{ normalize_null('solicitacao') }}), r'\s+', ' ') AS solicitacao,
    {{ normalize_null('area') }}                                                as area,
    upper(trim({{ normalize_null('sexo') }})) as sexo,


    -- Normaliza texto da idade e classifica idade em categorias limpas
    CASE
      WHEN {{ normalize_null('idade') }} IS NULL THEN NULL
      WHEN REGEXP_CONTAINS(LOWER(TRIM(CAST({{ normalize_null('idade') }} AS STRING))), r'\d') THEN NULL
      -- valores compostos tipo "idoso/adulto"
      WHEN REGEXP_CONTAINS(LOWER(TRIM(CAST({{ normalize_null('idade') }} AS STRING))), r'idos.*adult|adult.*idos') THEN 'adulto e idoso'
      WHEN REGEXP_CONTAINS(LOWER(TRIM(CAST({{ normalize_null('idade') }} AS STRING))), r'\brn\b|rec[eé]m[- ]?nascid') THEN 'rn'
      WHEN REGEXP_CONTAINS(LOWER(TRIM(CAST({{ normalize_null('idade') }} AS STRING))), r'idos') THEN 'idoso'
      WHEN REGEXP_CONTAINS(LOWER(TRIM(CAST({{ normalize_null('idade') }} AS STRING))), r'adult') THEN 'adulto'
      WHEN REGEXP_CONTAINS(LOWER(TRIM(CAST({{ normalize_null('idade') }} AS STRING))), r'crian[cç]a') THEN 'crianca'
      WHEN REGEXP_CONTAINS(LOWER(TRIM(CAST({{ normalize_null('idade') }} AS STRING))), r'adolesc') THEN 'adolescente'
      WHEN REGEXP_CONTAINS(LOWER(TRIM(CAST({{ normalize_null('idade') }} AS STRING))), r'n[uú]cleo\s+familiar|nucleo\s+familiar|fam[ií]li') THEN 'nucleo_familiar'
      ELSE NULL
    END AS idade,

    safe_cast({{ normalize_null('prazo_dias') }} as int64)                      as prazo_dias,
    
    {{ normalize_null('orgao_para_subsidiar') }}                                as orgao_para_subsidiar,
    {{ normalize_null('no_oficio') }}                                           as no_oficio,
    {{ normalize_null('observacoes') }}                                         as observacoes,
    {{ normalize_null('situacao') }}                                            as situacao

  FROM {{ source("brutos_cdi_staging", "judicial_residual") }}
)

select *
from base