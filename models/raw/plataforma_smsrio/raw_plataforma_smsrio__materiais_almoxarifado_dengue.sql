{{
    config(
        alias="materiais_almoxarifado_dengue",
    )
}}


with source as (
      select * from {{ source('brutos_plataforma_smsrio_staging', 'materiais_almoxarifado_dengue') }}
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
