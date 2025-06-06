{{
    config(
        alias="materiais_almoxarifado_dengue",
        tags="smsrio_estoque"
    )
}}


with source as (
      select * from {{ source('brutos_plataforma_smsrio_staging', 'subpav_arboviroses__itens_estoque') }}
),
renamed as (
    select
        {{ adapter.quote("id") }} as id_material,
        {{ adapter.quote("descricao") }},
        safe_cast({{ adapter.quote("created_at") }} as datetime) as created_at,
        safe_cast({{ adapter.quote("updated_at") }} as datetime) as updated_at

    from source
)
select * from renamed
