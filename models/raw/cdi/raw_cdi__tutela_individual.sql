{{
    config(
        schema = 'brutos_cdi',
        alias = 'equipe_tutela_individual',
        materialized = 'table'
    )
}}

with base as (

    select
        processorio__sei,
        no_oficio,
        orgao,
        procedimento,
        promotora_defensora,
        data_de_entrada,
        data_do_oficio,
        vencimento,
        data_de_saida,
        data_do_sms_ofi,
        objeto,
        assuntos,
        reiteracoes,
        area,
        sexo,
        idade,
        prazo_dias,
        orgao_para_subsidiar,
        no_oficio_sms,
        observacoes,
        situacao,
        mes
    from {{ source('brutos_cdi_staging', 'equipe_tutela_individual_2025') }}

    union all

    -- 2026
    select
        processorio__sei,
        no_oficio,
        orgao,
        procedimento,
        promotora_defensora,
        data_de_entrada,
        data_do_oficio,
        vencimento,
        data_de_saida,
        data_do_arquivamento as data_do_sms_ofi,
        objeto,
        assuntos,
        reiteracoes,
        area,
        sexo,
        idade,
        prazo_dias,
        orgao_para_subsidiar,
        no_oficio_sms,
        observacoes,
        situacao,
        mes
    from {{ source('brutos_cdi_staging', 'equipe_tutela_individual_2026') }}
),

fim as (

    select

        {{ normalize_null(
            "regexp_replace(trim(processorio__sei), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as processo_rio,

        {{ normalize_null(
            "regexp_replace(trim(no_oficio), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as no_oficio,

        {{ normalize_null(
            "regexp_replace(trim(orgao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as orgao,

        {{ normalize_null(
            "regexp_replace(trim(procedimento), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as procedimento,

        {{ normalize_null(
            "regexp_replace(trim(promotora_defensora), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as promotora_defensora,
        -- Datas

        {{ cdi_parse_date(
            process_null('data_de_entrada'),
            processo_field = process_null('processorio__sei'),
            oficio_field   = process_null('no_oficio')
        ) }} as data_de_entrada,

        {{ cdi_parse_date(
            process_null('data_do_oficio'),
            processo_field = process_null('processorio__sei'),
            oficio_field   = process_null('no_oficio')
        ) }} as data_do_oficio,

        {{ cdi_parse_date(
            process_null('vencimento'),
            processo_field = process_null('processorio__sei'),
            oficio_field   = process_null('no_oficio')
        ) }} as vencimento,

        {{ cdi_parse_date(
            process_null('data_de_saida'),
            processo_field = process_null('processorio__sei'),
            oficio_field   = process_null('no_oficio')
        ) }} as data_de_saida,

        {{ cdi_parse_date(
            process_null('data_do_sms_ofi'),
            processo_field = process_null('processorio__sei'),
            oficio_field   = process_null('no_oficio')
        ) }} as data_do_sms_ofi,

        -- Outros campos
        {{ normalize_null(
            "regexp_replace(trim(objeto), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as objeto,

        {{ normalize_null(
            "regexp_replace(trim(assuntos), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as assuntos,

        {{ normalize_null(
            "regexp_replace(trim(reiteracoes), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as reiteracoes,

        {{ normalize_null(
            "regexp_replace(trim(area), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as area,

        upper(trim({{ normalize_null('sexo') }})) as sexo,

    
        case
            when regexp_contains(lower(trim({{ normalize_null('idade') }})), r'idos')
                then 'Idoso'
            
            when regexp_contains(lower(trim({{ normalize_null('idade') }})), r'crian')
                or regexp_contains(lower(trim({{ normalize_null('idade') }})), r'infant')
                or regexp_contains(lower(trim({{ normalize_null('idade') }})), r'adolesc')
                then 'Criança/Adolescente'
            
            when regexp_contains(lower(trim({{ normalize_null('idade') }})), r'adult')
                or lower(trim({{ normalize_null('idade') }})) = 'adulo'
                then 'Adulto'
            
            when regexp_contains(lower(trim({{ normalize_null('idade') }})), r'n[uú]cleo\s+familiar')
                then 'Núcleo Familiar'
            
            when regexp_contains(lower(trim({{ normalize_null('idade') }})), r'n[aã]o\s+identif')
                then 'Não Identificado'
            
            else 'Ignorado'
        end as classificacao_idade,


        safe_cast(
            {{ normalize_null('prazo_dias') }} as int64
        ) as prazo_dias,

        {{ normalize_null(
            "regexp_replace(trim(orgao_para_subsidiar), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as orgao_para_subsidiar,

        {{ normalize_null(
            "regexp_replace(trim(no_oficio_sms), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as no_oficio_sms,

        {{ normalize_null(
            "regexp_replace(trim(observacoes), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as observacoes,

        {{ normalize_null(
            "regexp_replace(trim(situacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as situacao,

        {{ normalize_null(
            "regexp_replace(trim(mes), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as mes

    from base
)

select *
from fim
where processo_rio is not null 
        and orgao is not null
