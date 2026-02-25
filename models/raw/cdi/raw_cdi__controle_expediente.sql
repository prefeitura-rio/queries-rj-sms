{{
    config(
        schema='brutos_cdi',
        alias='controle_expediente',
        materialized='table'
    )
}}

with base as (

    -- 2026
    select
        data,
        pessoa_de_cadastro,
        processo,
        tipo_de_documento,
        nome_do_paciente,
        orgao_de_origem,
        tipo_de_solicitacao,
        pacientes_novos,
        direcionamento_interno,
        sem_deferimento_ou_extinto,
        cap,
        sexo,
        estadual_federal,
        advogado_particular__defensoria_publica,
        valor_r_,
        n__do_documento,
        multas,
        prazos,
        data_de_saida,
        responsavel_pela_saida,
        juridico
    from {{ source('brutos_cdi_staging', 'controle_expediente_2026') }}

),

renamed as (
    select

        -- Identificadores
        {{ normalize_null(
            "regexp_replace(trim(processo), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as processo,

        {{ normalize_null(
            "regexp_replace(trim(tipo_de_documento), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as tipo_de_documento,

        {{ normalize_null(
            "regexp_replace(trim(n__do_documento), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as num_documento,

        -- Pessoas
        {{ normalize_null(
            "regexp_replace(trim(pessoa_de_cadastro), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as pessoa_de_cadastro,

        {{ normalize_null(
            "regexp_replace(trim(nome_do_paciente), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as nome_do_paciente,

        {{ normalize_null(
            "regexp_replace(trim(responsavel_pela_saida), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as responsavel_pela_saida,

        -- Órgão e origem
        {{ normalize_null(
            "regexp_replace(trim(orgao_de_origem), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as orgao_de_origem,

        -- Classificações
        {{ normalize_null(
            "regexp_replace(trim(tipo_de_solicitacao), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as tipo_de_solicitacao,

        {{ normalize_null(
            "regexp_replace(trim(pacientes_novos), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as pacientes_novos,

        {{ normalize_null(
            "regexp_replace(trim(direcionamento_interno), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as direcionamento_interno,

        {{ normalize_null(
            "regexp_replace(trim(sem_deferimento_ou_extinto), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as sem_deferimento_ou_extinto,

        {{ normalize_null(
            "regexp_replace(trim(cap), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as cap,

        {{ normalize_null(
            "regexp_replace(trim(sexo), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as sexo,

        {{ normalize_null(
            "regexp_replace(trim(estadual_federal), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as estadual_federal,

        {{ normalize_null(
            "regexp_replace(trim(advogado_particular__defensoria_publica), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as advogado_ou_defensoria,

        {{ normalize_null(
            "regexp_replace(trim(juridico), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as juridico,

        -- Valores
        {{ normalize_null(
            "regexp_replace(trim(valor_r_), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as valor_reais,

        {{ normalize_null(
            "regexp_replace(trim(multas), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as multas,

        -- Datas
        {{ cdi_parse_date(
            process_null('data'),
            processo_field=process_null('processo'),
            oficio_field=process_null('tipo_de_documento')
        ) }} as data_entrada,

        {{ cdi_parse_date(
            process_null('prazos'),
            processo_field=process_null('processo'),
            oficio_field=process_null('tipo_de_documento')
        ) }} as prazos,

        {{ cdi_parse_date(
            process_null('data_de_saida'),
            processo_field=process_null('processo'),
            oficio_field=process_null('tipo_de_documento')
        ) }} as data_de_saida

    from base
)

select *
from renamed
where processo is not null
    and orgao_de_origem is not null