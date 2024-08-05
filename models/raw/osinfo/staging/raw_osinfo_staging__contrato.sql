{{
    config(
        alias="contrato",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'contrato') }}
),
renamed as (
    select
        {{ adapter.quote("id_contrato") }},
        {{ adapter.quote("numero_contrato") }},
        {{ adapter.quote("cod_organizacao") }},
        {{ adapter.quote("data_atualizacao") }},
        {{ adapter.quote("data_assinatura") }},
        {{ adapter.quote("periodo_vigencia") }},
        {{ adapter.quote("data_publicacao") }},
        {{ adapter.quote("data_inicio") }},
        {{ adapter.quote("valor_total") }},
        {{ adapter.quote("valor_ano1") }},
        {{ adapter.quote("valor_parcelas") }},
        {{ adapter.quote("valor_fixo") }},
        {{ adapter.quote("valor_variavel") }},
        {{ adapter.quote("observacao") }},
        {{ adapter.quote("ap") }},
        {{ adapter.quote("id_secretaria") }}

    from source
)
select * from renamed
  