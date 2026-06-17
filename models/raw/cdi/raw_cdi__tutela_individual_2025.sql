{{
    config(
        schema = "brutos_cdi",
        alias = "tutela_individual_2025",
        materialized = "table"
    )
}}

select
    {{ normalize_null("regexp_replace(regexp_replace(trim(mes), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as mes,

    {{ cdi_parse_date('data_de_entrada', 'processo_rio__sei', 'no_oficio') }} as data_entrada,

    {{ normalize_null("regexp_replace(regexp_replace(trim(processo_rio__sei), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as processo_rio,

    {{ normalize_null("regexp_replace(regexp_replace(trim(no_oficio), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as no_oficio,

    {{ cdi_parse_date('data_do_oficio', 'processo_rio__sei', 'no_oficio') }} as data_oficio,

    {{ normalize_null("regexp_replace(regexp_replace(trim(orgao), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as orgao,

    {{ normalize_null("regexp_replace(regexp_replace(trim(procedimento), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as procedimento,

    {{ normalize_null("regexp_replace(regexp_replace(trim(promotor_a__defensor_a), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as promotor_defensor,

    {{ normalize_null("regexp_replace(regexp_replace(trim(objeto), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as objeto,

    {{ normalize_null("regexp_replace(regexp_replace(trim(assuntos), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as assuntos,

    {{ normalize_null("regexp_replace(regexp_replace(trim(reiteracoes), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as reiteracoes,

    {{ normalize_null("regexp_replace(regexp_replace(trim(area), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as area,

    upper(trim({{ normalize_null('sexo') }})) as sexo,

    {{ normalize_null("regexp_replace(regexp_replace(trim(idade), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as idade,

    safe_cast({{ normalize_null('prazo__dias') }} as int64) as prazo_dias,

    {{ cdi_parse_date('vencimento', 'processo_rio__sei', 'no_oficio') }} as vencimento,

    {{ cdi_parse_date('data_de_saida', 'processo_rio__sei', 'no_oficio') }} as data_saida,

    {{ normalize_null("regexp_replace(regexp_replace(trim(orgao_para_subsidiar), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as orgao_para_subsidiar,

    {{ normalize_null("regexp_replace(regexp_replace(trim(no_oficio_sms), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as no_oficio_sms,

    {{ cdi_parse_date('data_do_sms_ofi', 'processo_rio__sei', 'no_oficio') }} as data_sms_ofi,

    {{ normalize_null("regexp_replace(regexp_replace(trim(observacoes), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as observacoes,

    {{ normalize_null("regexp_replace(regexp_replace(trim(situacao), r'(?i)^(#ref!|#value!)$', ''), r'\\s+', ' ')") }} as situacao

from {{ source("brutos_cdi_staging", "equipe_tutela_individual_2025") }}