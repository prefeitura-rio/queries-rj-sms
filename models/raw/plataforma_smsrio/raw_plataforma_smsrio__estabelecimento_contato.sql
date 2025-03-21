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
            timestamp_add(datetime(timestamp({{process_null('created_at')}}), 'America/Sao_Paulo'),interval 3 hour) as created_at,
            timestamp_add(datetime(timestamp({{process_null('updated_at')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
        from source
    )
select *
from renamed
