{{
    config(
        schema = 'brutos_cdi',
        alias = 'demanda_interna_externa_derivados',
        materialized = 'table'
    )
}}

with base as (
    select
        *
    from {{ source('brutos_cdi_staging', 'controle_demanda_interna_externa_derivados') }}
    where (data_de_emissao is not null and data_de_emissao != "") -- tratando vazios e nulos
       or (derivado_do_processorio is not null and derivado_do_processorio != "")
)

select
    
    {{ normalize_null("regexp_replace(trim(id), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as id,

    {{ normalize_null("regexp_replace(trim(cadastrado_por), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as cadastrado_por,

    -- Data de emissão
    {{ cdi_parse_date(
            process_null('data_de_emissao'),
            processo_field = process_null('processorio'),
            oficio_field   = process_null('derivado_do_processorio') 
        ) }} as data_de_emissao,

    {{ normalize_null("regexp_replace(trim(processorio), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as processorio,

    {{ normalize_null("regexp_replace(trim(breve_descricao_da_solicitacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as breve_descricao_da_solicitacao,

    {{ normalize_null("regexp_replace(trim(derivado_do_processorio), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as derivado_do_processorio,

    -- Data de vencimento
    {{ cdi_parse_date(
            process_null('vencimento'),
            processo_field = process_null('processorio'),
            oficio_field   = process_null('derivado_do_processorio')
        ) }} as vencimento,

    -- Setor e acompanhamento
    {{ normalize_null("regexp_replace(trim(subsecretaria___setor), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as subsecretaria___setor,

    {{ cdi_parse_date(
            process_null('data_da_ultima_atualizacao'),
            processo_field = process_null('processorio'),
            oficio_field   = process_null('derivado_do_processorio')
        ) }} as data_da_ultima_atualizacao,

    {{ normalize_null("regexp_replace(trim(prazo_para_retorno), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as prazo_para_retorno,

    {{ normalize_null("regexp_replace(trim(atrazo_em_dias), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as atraso_em_dias,

    {{ normalize_null("regexp_replace(trim(status), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as status,

    {{ normalize_null("regexp_replace(trim(observacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as observacao

from base