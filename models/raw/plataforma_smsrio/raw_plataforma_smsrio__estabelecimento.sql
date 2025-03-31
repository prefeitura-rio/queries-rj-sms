{{
    config(
        alias="estabelecimento",
    )
}}

with
    source as (
        select * from {{ source('brutos_plataforma_smsrio_staging','subpav_cnes__unidades') }}
    ),
    most_recent as (
        select * from source
        qualify row_number() over (partition by id order by updated_at desc) = 1
    ),
    renamed as (
        select
            cast(id as int64) as id,
            format("%07d", cast(cnes as int64)) as id_cnes,
            * except (id, cnes, created_at, updated_at),
            timestamp_add(datetime(timestamp({{process_null('created_at')}}), 'America/Sao_Paulo'),interval 3 hour) as created_at,
            timestamp_add(datetime(timestamp({{process_null('updated_at')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
        from most_recent
    )
select *
from renamed
