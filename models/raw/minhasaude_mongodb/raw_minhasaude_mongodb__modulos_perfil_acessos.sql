{{
    config(
        schema="brutos_minhasaude_mongodb",
        alias="modulos_perfil_acessos",
        cluster_by=['_id'],
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    source as (
        select
            _id,

            rota,
            nome,
            descricao,

            safe_cast(createdat as datetime) as createdat,
            safe_cast(updatedat as datetime) as updatedat,
            __v,

            date(safe_cast(data_extracao as timestamp), 'America/Sao_Paulo') as data_extracao,
            
            row_number() over (partition by _id order by data_particao desc) as rn

        from {{ source("brutos_minhasaude_mongodb_staging", "modulos_perfil_acessos") }}
        qualify rn = 1
    )

select *
from source
