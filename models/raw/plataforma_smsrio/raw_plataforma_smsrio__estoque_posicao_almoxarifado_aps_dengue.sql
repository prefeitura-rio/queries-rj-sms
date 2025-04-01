{{
    config(
        alias="estoque_posicao_almoxarifado_aps_dengue",
        tags="smsrio_estoque"
    )
}}

with
    source as (
        select *
        from
            {{
                source(
                    "brutos_plataforma_smsrio_staging",
                    "subpav_arboviroses__estoque",
                )
            }}
    ),
    casted as (
        select
            {{ adapter.quote("id") }},
            safe_cast({{ adapter.quote("data") }} as date) as data,
            {{ adapter.quote("item_id") }} as id_material,
            format("%07d", cast({{ adapter.quote("cnes") }} as int64)) as id_cnes,
            safe_cast({{ adapter.quote("valor") }} as int64) as material_quantidade,
            {{ adapter.quote("user_id") }} id_usuario_atualizacao,
            safe_cast({{ adapter.quote("created_at") }} as datetime) as created_at,
            safe_cast({{ adapter.quote("updated_at") }} as datetime) as updated_at

        from source
    )
select
    -- Primary key
    id,

    -- Foreign keys
    id_cnes,
    id_material,

    -- Fields
    data,
    material_quantidade,

    -- Metadata
    id_usuario_atualizacao,
    created_at,
    updated_at
from casted
