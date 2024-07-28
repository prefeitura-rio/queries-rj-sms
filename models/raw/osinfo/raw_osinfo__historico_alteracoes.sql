{{
    config(
        alias="historico_alteracoes",
    )
}}


with source as (
      select * from {{ source('osinfo', 'historico_alteracoes') }}
),
renamed as (
    select
        {{ adapter.quote("id_historico_alteracoes") }},
        {{ adapter.quote("id_tipo_arquivo") }},
        {{ adapter.quote("cod_usuario") }},
        {{ adapter.quote("cod_organizacao") }},
        {{ adapter.quote("data_modificacao") }},
        {{ adapter.quote("valor_anterior") }},
        {{ adapter.quote("valor_novo") }},
        {{ adapter.quote("mes_referencia") }},
        {{ adapter.quote("ano_referencia") }},
        {{ adapter.quote("id_registro") }},
        {{ adapter.quote("tipo_alteracao") }}

    from source
)
select * from renamed
  