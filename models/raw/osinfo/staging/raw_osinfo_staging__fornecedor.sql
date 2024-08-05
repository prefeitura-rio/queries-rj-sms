{{
    config(
        alias="fornecedor",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'fornecedor') }}
),
renamed as (
    select
        {{ adapter.quote("id_fornecedor") }},
        {{ adapter.quote("cod_fornecedor") }},
        {{ adapter.quote("fornecedor") }},
        {{ adapter.quote("tipo_pessoa") }},
        {{ adapter.quote("endereco") }},
        {{ adapter.quote("numero") }},
        {{ adapter.quote("complemento") }},
        {{ adapter.quote("cep") }},
        {{ adapter.quote("bairro") }},
        {{ adapter.quote("municipio") }},
        {{ adapter.quote("uf") }},
        {{ adapter.quote("referencia") }},
        {{ adapter.quote("telefone_1") }},
        {{ adapter.quote("telefone_1_ramal") }},
        {{ adapter.quote("telefone_2") }},
        {{ adapter.quote("telefone_2_ramal") }},
        {{ adapter.quote("telefone_fax") }},
        {{ adapter.quote("email") }},
        {{ adapter.quote("contato") }},
        {{ adapter.quote("id_log_importacao") }},
        {{ adapter.quote("cod_organizacao") }},
        {{ adapter.quote("data_envio") }}

    from source
)
select * from renamed
  