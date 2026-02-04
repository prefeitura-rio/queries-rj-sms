{{
    config(
        schema='brutos_cdi',
        alias='equipe_tutela_coletiva',
        materialized='table'
    )
}}

with base as (

    -- 2025
    select
        processorio___sei as sei,        -- processo
        oficio,
        orgao,
        reiteracoes,
        ic,
        pa,
        referencia,
        no_documento,
        data_da_entrada,
        vencimento,
        data_de_saida,
        data_do_envio_ao_orgao_solicitante___arquivamento,
        prazo_dias,
        assunto,
        sintese_da_solicitacao,
        unidade,
        area,
        sei as sei_status,               -- status
        orgao_para_subsidiar,
        exigencia,
        oficio_sms,
        observacoes,
        status
    from {{ source('brutos_cdi_staging', 'equipe_tutela_coletiva_2025') }}

    union all

    -- 2026
    select
        sei,                    
        oficio,
        orgao,
        reiteracoes,
        ic,
        pa,
        referencia,
        no_documento,
        data_da_entrada,
        vencimento,
        data_de_saida,
        data_do_envio_ao_orgao_solicitante___arquivamento,
        prazo_dias,
        assunto,
        sintese_da_solicitacao,
        unidade,
        area,
        sei_status as sei_status,       -- status
        orgao_para_subsidiar,
        exigencia,
        oficio_sms,
        observacoes,
        status
    from {{ source('brutos_cdi_staging', 'equipe_tutela_coletiva_2026') }}

),

renamed as (
    select
       
        -- Identificadores
        {{ normalize_null(
            "regexp_replace(trim(sei), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as processo_rio,

        {{ normalize_null(
            "regexp_replace(trim(oficio), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as num_oficio,

        {{ normalize_null(
            "regexp_replace(trim(orgao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as orgao,

        {{ normalize_null(
            "regexp_replace(trim(reiteracoes), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as reiteracoes,

        {{ normalize_null(
            "regexp_replace(trim(ic), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as ic,

        {{ normalize_null(
            "regexp_replace(trim(pa), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as pa,

        {{ normalize_null(
            "regexp_replace(trim(referencia), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as referencia,

        {{ normalize_null(
            "regexp_replace(trim(no_documento), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as no_documento,

        -- Datas


        {{ cdi_parse_date(
            process_null('data_da_entrada'),
            processo_field=process_null('sei'),
            oficio_field=process_null('oficio')
        ) }} as data_da_entrada,

        {{ cdi_parse_date(
            process_null('vencimento'),
            processo_field=process_null('sei'),
            oficio_field=process_null('oficio')
        ) }} as vencimento,

        {{ cdi_parse_date(
            process_null('data_de_saida'),
            processo_field=process_null('sei'),
            oficio_field=process_null('oficio')
        ) }} as data_de_saida,


        {{ cdi_parse_date(
            process_null('data_do_envio_ao_orgao_solicitante___arquivamento'),
            processo_field=process_null('sei'),
            oficio_field=process_null('oficio')
        ) }} as data_envio_orgao_solicitante_arquivamento,

        -- Campos gerais
        {{ normalize_null(
            "regexp_replace(trim(prazo_dias), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as prazo_dias,

        {{ normalize_null(
            "regexp_replace(trim(assunto), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as assunto,



        {{ normalize_null(
            "regexp_replace(trim(sintese_da_solicitacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as sintese_da_solicitacao,

        {{ normalize_null(
            "regexp_replace(trim(unidade), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as unidade,

        {{ normalize_null(
            "regexp_replace(trim(area), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as area,

        {{ normalize_null(
            "regexp_replace(trim(sei_status), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as sei_status,

        {{ normalize_null(
            "regexp_replace(trim(orgao_para_subsidiar), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as orgao_para_subsidiar,

        {{ normalize_null(
            "regexp_replace(trim(exigencia), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as exigencia,

        {{ normalize_null(
            "regexp_replace(trim(oficio_sms), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as oficio_sms,

        {{ normalize_null(
            "regexp_replace(trim(observacoes), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as observacoes,

        {{ normalize_null(
            "regexp_replace(trim(status), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as status

    from base
)

select *
from renamed
