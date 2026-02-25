{{
    config(
        schema='brutos_cdi',
        alias='atendimento_mandado_ata_vigente',
        materialized='table'
    )
}}

with base as (

    select
        processo_de_compra,
        no_do_item,
        codigo_do_item,
        objeto,
        qtd_ata,
        valor_unitario,
        empresa_vencedora,
        no_pe,
        no_ata,
        inicio,
        fim,
        pedidos,
        utilizado,
        saldo,
        obs
    from {{ source('brutos_cdi_staging', 'atendimento_mandado_atas_vigentes') }}

),

base_t as (
    select

        -- Identificadores do processo/ata
        {{ normalize_null(
            "regexp_replace(trim(processo_de_compra), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as processo_de_compra,

        {{ normalize_null(
            "regexp_replace(trim(no_do_item), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as num_item,

        {{ normalize_null(
            "regexp_replace(trim(codigo_do_item), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as codigo_item,

        {{ normalize_null(
            "regexp_replace(trim(no_pe), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as num_pe,

        {{ normalize_null(
            "regexp_replace(trim(no_ata), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as num_ata,

        -- Descrição do objeto
        {{ normalize_null(
            "regexp_replace(trim(objeto), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as objeto,

        -- Empresa
        {{ normalize_null(
            "regexp_replace(trim(empresa_vencedora), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as empresa_vencedora,

        -- Quantidades e valores
        {{ normalize_null(
            "regexp_replace(trim(qtd_ata), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as qtd_ata,

        {{ normalize_null(
            "regexp_replace(trim(valor_unitario), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as valor_unitario,

        {{ normalize_null(
            "regexp_replace(trim(pedidos), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as pedidos,

        {{ normalize_null(
            "regexp_replace(trim(utilizado), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as utilizado,

        {{ normalize_null(
            "regexp_replace(trim(saldo), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as saldo,

        -- Datas de vigência
        {{ cdi_parse_date(
            process_null('inicio'),
            processo_field=process_null('processo_de_compra'),
            oficio_field=process_null('no_ata')
        ) }} as data_inicio,

        {{ cdi_parse_date(
            process_null('fim'),
            processo_field=process_null('processo_de_compra'),
            oficio_field=process_null('no_ata')
        ) }} as data_fim,

        -- Observações
        {{ normalize_null(
            "regexp_replace(trim(obs), r'(?i)^(x|-|#ref!)$|[\\n\\r\\t]', '')"
        ) }} as observacao

    from base
)

select *
from base_t
