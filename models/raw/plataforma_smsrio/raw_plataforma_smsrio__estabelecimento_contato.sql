{{
    config(
        alias="estabelecimento_contato",
    )
}}

with
    source as (
        select * from {{ source('brutos_plataforma_smsrio_staging','subpav_cnes__contatos_unidades') }}
    ),
    most_recent as (
        select * from source
        qualify row_number() over (partition by unidade_id order by updated_at desc) = 1
    ),
    renamed as (
        select
            cast(unidade_id as int64) as unidade_id,
            split({{process_null('telefone')}}, "|") as telefone,
            {{process_null('email')}} as email,
            {{process_null('facebook')}} as facebook,
            {{process_null('instagram')}} as instagram,
            {{process_null('twitter')}} as twitter,
            timestamp_add(datetime(timestamp({{process_null('created_at')}}), 'America/Sao_Paulo'),interval 3 hour) as created_at,
            timestamp_add(datetime(timestamp({{process_null('updated_at')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
        from most_recent
    )
select *
from renamed
