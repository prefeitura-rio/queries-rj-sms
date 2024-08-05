{{
    config(
        alias="plano_contas",
        schema="brutos_osinfo_staging"
    )
}}


with source as (
      select * from {{ source('osinfo', 'plano_contas') }}
),
renamed as (
    select
        {{ adapter.quote("id_item_plano_de_contas") }},
        {{ adapter.quote("cod_item_plano_de_contas") }},
        {{ adapter.quote("descricao_item_plano_de_contas") }},
        {{ adapter.quote("id_item_plano_de_contas_n1") }},
        {{ adapter.quote("id_item_plano_de_contas_n2") }},
        {{ adapter.quote("flg_ativo") }}

    from source
)
select * from renamed
  