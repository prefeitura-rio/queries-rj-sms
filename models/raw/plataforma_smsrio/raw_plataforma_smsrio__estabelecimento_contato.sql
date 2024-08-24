{{
    config(
        alias="estabelecimento_contato",
    )
}}

with
    source as (
        select *
        from {{ source("brutos_plataforma_smsrio_staging", "estabelecimento_contato") }}
    ),
    renamed as (
        select
            format("%07d", cast(cnes as int64)) as id_cnes,
            -- unidade_id,
            split(telefone, "|") as telefone,
            email,
            -- user_id,
            facebook,
            instagram,
            twitter,
            -- dt_inaugura,
            created_at,
            updated_at,
        from source
    )
select *
from renamed
