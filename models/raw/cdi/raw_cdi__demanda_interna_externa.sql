{{
    config(
        schema = 'brutos_cdi_staging',
        alias = 'demanda_interna_externa',
        materialized = 'table'
    )
}}


with base as (

    select
        *
    from {{ source('brutos_cdi_staging', 'controle_demandas_interno_externo') }}
    where (data_de_entrada is not null and data_de_entrada != "") -- tratando vazios e nulos
       or (orgao_demandante is not null and orgao_demandante != "")


)

select

    -- Identificador
    {{ normalize_null("regexp_replace(trim(id), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as id,

    {{ normalize_null("regexp_replace(trim(cadastrado_por), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as cadastrado_por,

    -- Datas principais
            {{ cdi_parse_date(
            process_null('data_de_entrada'),
            processo_field = process_null('processorio_sei'),
            oficio_field   = process_null('no_da_manifestacao')
        ) }} as data_de_entrada,

    -- Órgão
    {{ normalize_null("regexp_replace(trim(orgao_demandante), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as orgao_demandante,

    {{ normalize_null("regexp_replace(trim(processorio_sei), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as processorio_sei,

    {{ normalize_null("regexp_replace(trim(referencia), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as referencia,

    {{ normalize_null("regexp_replace(trim(tipo_de_demanda), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as tipo_de_demanda,

    {{ normalize_null("regexp_replace(trim(manifestacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as manifestacao,

    {{ normalize_null("regexp_replace(trim(no_da_manifestacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as no_da_manifestacao,

    {{ normalize_null("regexp_replace(trim(relator_auditor), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as relator_auditor,

    {{ normalize_null("regexp_replace(trim(decisao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as decisao,

    {{ normalize_null("regexp_replace(trim(na_da_natureza_juridica), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as na_da_natureza_juridica,

    {{ normalize_null("regexp_replace(trim(natureza_juridica), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as natureza_juridica,

    {{ normalize_null("regexp_replace(trim(contratado), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as contratado,

    {{ normalize_null("regexp_replace(trim(unidade_ap), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as unidade_ap,

    {{ normalize_null("regexp_replace(trim(descricao_objeto), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as descricao_objeto,

    -- Datas de vencimento
    {{ cdi_parse_date(
            process_null('vencimento_1'),
            processo_field = process_null('processorio_sei'),
            oficio_field   = process_null('no_da_manifestacao')
        ) }} as vencimento_1,
    -- vencimento 2
    {{ cdi_parse_date(
            process_null('vencimento_2'),
            processo_field = process_null('processorio_sei'),
            oficio_field   = process_null('no_da_manifestacao')
        ) }} as vencimento_2,

    {{ normalize_null("regexp_replace(trim(subsecretaria___setor), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as subsecretaria___setor,

    {{ cdi_parse_date(
            process_null('data_da_ultima_atualizacao'),
            processo_field = process_null('processorio_sei'),
            oficio_field   = process_null('no_da_manifestacao')
        ) }} as data_da_ultima_atualizacao,

    {{ normalize_null("regexp_replace(trim(prazo_para_retorno_gat_2), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as prazo_para_retorno_gat_2,

    {{ normalize_null("regexp_replace(trim(atraso_em_dias___venc_1), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as atraso_em_dias___venc_1,

    {{ normalize_null("regexp_replace(trim(atraso_em_dias__venc_2), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as atraso_em_dias__venc_2,

    {{ normalize_null("regexp_replace(trim(status), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as status,

    {{ normalize_null("regexp_replace(trim(oficio_de_dilacao_de_prazo__solicitacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as oficio_de_dilacao_de_prazo__solicitacao,

    -- data_da_solicitacao_de_prazo cdi parse date
    {{ cdi_parse_date(
            process_null('data_da_solicitacao_de_prazo'),
            processo_field = process_null('processorio_sei'),
            oficio_field   = process_null('no_da_manifestacao')
        ) }} as data_da_solicitacao_de_prazo,

    {{ normalize_null("regexp_replace(trim(oficio_resposta_de_dilacao_de_prazo), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as oficio_resposta_de_dilacao_de_prazo,

    {{ cdi_parse_date(
            process_null('data_prorrogada'),
            processo_field = process_null('processorio_sei'),
            oficio_field   = process_null('no_da_manifestacao')
        ) }} as data_prorrogada,

    {{ normalize_null("regexp_replace(trim(numero_de_dilacoes_solicitadas), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as numero_de_dilacoes_solicitadas,

    {{ normalize_null("regexp_replace(trim(observacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')") }} as observacao

from base
