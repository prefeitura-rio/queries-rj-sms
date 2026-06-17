{{ config(
    schema = "brutos_cdi",
    alias  = "judicial_residual_2025",
    materialized = "table"
) }}

select
  {{ normalize_null("regexp_replace(trim(processo_rio__sei), r'[\\n\\r]', '')") }} as processo_rio,

  {{ normalize_null("upper(regexp_replace(trim(mrj_e_parte), r'[\\n\\r\\t]+', ''))") }} as envolve_mrj,

  {{ normalize_null("regexp_replace(trim(oficio), r'[\\n\\r\\t]+', '')") }} as oficio,

  {{ cdi_parse_date('data', 'processo_rio__sei', 'oficio') }} as data_oficio_origem,

  {{ normalize_null("regexp_replace(trim(demandante), r'[\\n\\r\\t]+', '')") }} as demandante,

  {{ normalize_null("regexp_replace(trim(orgao), r'[\\n\\r\\t]+', '')") }} as orgao,

  {{ normalize_null("regexp_replace(trim(processo), r'[\\n\\r\\t]+', '')") }} as processo,

  regexp_replace(trim({{ normalize_null('assunto') }}), r'\s+', ' ') as assunto,

  regexp_replace(trim({{ normalize_null('solicitacao') }}), r'\s+', ' ') as solicitacao,

  {{ normalize_null("regexp_replace(trim(area), r'[\\n\\r\\t]+', '')") }} as area,

  upper(trim({{ normalize_null('sexo') }})) as sexo,

  {{ normalize_null("regexp_replace(trim(idade), r'[\\n\\r\\t]+', '')") }} as idade,

  {{ cdi_parse_date('entrada_gat_3', 'processo_rio__sei', 'oficio') }} as entrada_gat3,

  safe_cast({{ normalize_null('prazo__dias') }} as int64) as prazo_dias,

  {{ cdi_parse_date('vencimento', 'processo_rio__sei', 'oficio') }} as vencimento,

  {{ cdi_parse_date('data_de_saida', 'processo_rio__sei', 'oficio') }} as data_saida,

  {{ normalize_null("regexp_replace(trim(orgao_para_subsidiar), r'[\\n\\r\\t]+', '')") }} as orgao_para_subsidiar,

  {{ cdi_parse_date('retorno', 'processo_rio__sei', 'oficio') }} as retorno,

  {{ normalize_null("regexp_replace(trim(no_oficio), r'[\\n\\r\\t]+', '')") }} as no_oficio,

  {{ cdi_parse_date('data_do_oficio', 'processo_rio__sei', 'oficio') }} as data_oficio,

  {{ cdi_parse_date('pg_pas_dta_sfc', 'processo_rio__sei', 'oficio') }} as data_pg_pas_dta_sfc,

  regexp_replace(trim({{ normalize_null('observacoes') }}), r'\s+', ' ') as observacoes,

  case
    when lower(trim(cast(situacao as string))) in ('#ref!', '#value!')
      then null
    else {{ normalize_null("regexp_replace(trim(situacao), r'[\\n\\r\\t]+', '')") }}
  end as situacao

from {{ source("brutos_cdi_staging", "judicial_residual_2025") }}