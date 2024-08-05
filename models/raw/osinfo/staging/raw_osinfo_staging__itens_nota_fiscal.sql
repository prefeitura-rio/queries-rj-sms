{{
    config(
        alias="itens_nota_fiscal",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'itens_nota_fiscal') }}
),
renamed as (
    select
        {{ adapter.quote("id_item_nf") }},
        {{ adapter.quote("cod_item_nf") }},
        {{ adapter.quote("qtd_material") }},
        {{ adapter.quote("valor_unitario") }},
        {{ adapter.quote("referencia_mes_nf") }},
        {{ adapter.quote("referencia_ano_nf") }},
        {{ adapter.quote("id_fornecedor") }},
        {{ adapter.quote("valor_total") }},
        {{ adapter.quote("num_documento") }},
        {{ adapter.quote("cod_instituicao") }},
        {{ adapter.quote("data_envio") }},
        {{ adapter.quote("tipo_item") }},
        {{ adapter.quote("item") }},
        {{ adapter.quote("unidade_medida") }},
        {{ adapter.quote("observacao") }}

    from source
)
select * from renamed
  