{{
    config(
        schema = "brutos_cdi",
        alias = "tutela_coletiva_2025",
        materialized = "table"
    )
}}

select
    {{ normalize_null("regexp_replace(regexp_replace(trim(processo_rio__sei), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as processo_rio,
    {{ normalize_null("regexp_replace(regexp_replace(trim(oficio), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as oficio,
    {{ normalize_null("regexp_replace(regexp_replace(trim(orgao), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as orgao,
    {{ normalize_null("regexp_replace(regexp_replace(trim(reiteracoes), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as reiteracoes,
    {{ normalize_null("regexp_replace(regexp_replace(trim(ic), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as ic,
    {{ normalize_null("regexp_replace(regexp_replace(trim(pa), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as pa,
    {{ normalize_null("regexp_replace(regexp_replace(trim(referencia), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as referencia,
    {{ normalize_null("regexp_replace(regexp_replace(trim(no_documento), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as no_documento,

    {{ cdi_parse_date('data_do_of', 'processo_rio__sei', 'oficio') }} as data_of,
    {{ cdi_parse_date('data_da_entrada', 'processo_rio__sei', 'oficio') }} as data_entrada,
    safe_cast({{ normalize_null('prazo__dias') }} as int64) as prazo_dias,
    {{ cdi_parse_date('vencimento', 'processo_rio__sei', 'oficio') }} as vencimento,
    {{ cdi_parse_date('data_de_saida', 'processo_rio__sei', 'oficio') }} as data_saida,
    {{ cdi_parse_date('data_do_oficio', 'processo_rio__sei', 'oficio') }} as data_oficio,
    {{ cdi_parse_date('data_do_envio_ao_orgao_solicitante__arquivamento', 'processo_rio__sei', 'oficio') }} as data_envio_orgao_solicitante_arquivamento,

    {{ normalize_null("regexp_replace(regexp_replace(trim(assunto), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as assunto,
    {{ normalize_null("regexp_replace(regexp_replace(trim(solicitacao), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as solicitacao,
    {{ normalize_null("regexp_replace(regexp_replace(trim(sintese_da_solicitacao), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as sintese_solicitacao,
    {{ normalize_null("regexp_replace(regexp_replace(trim(unidade), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as unidade,
    {{ normalize_null("regexp_replace(regexp_replace(trim(area), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as area,

    {{ normalize_null("regexp_replace(regexp_replace(trim(sei), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as sei_status,
    {{ normalize_null("regexp_replace(regexp_replace(trim(orgao_para_subsidiar), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as orgao_para_subsidiar,
    {{ normalize_null("regexp_replace(regexp_replace(trim(retorno), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as retorno,
    {{ normalize_null("regexp_replace(regexp_replace(trim(exigencia), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as exigencia,
    {{ normalize_null("regexp_replace(regexp_replace(trim(retorno_2), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as retorno_2,
    {{ normalize_null("regexp_replace(regexp_replace(trim(oficio_sms), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as oficio_sms,
    {{ normalize_null("regexp_replace(regexp_replace(trim(observacoes), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as observacoes,
    {{ normalize_null("regexp_replace(regexp_replace(trim(status), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as status

from {{ source("brutos_cdi_staging", "equipe_tutela_coletiva_2025") }}