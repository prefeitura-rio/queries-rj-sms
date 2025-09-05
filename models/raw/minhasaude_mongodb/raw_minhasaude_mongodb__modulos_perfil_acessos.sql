{{
    config(
        schema="brutos_minhasaude_mongodb",
        alias="modulos_perfil_acessos",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    last_partition as (
        select max(data_extracao) as max_partition from {{ source("brutos_minhasaude_mongodb_staging", "modulos_perfil_acessos") }}

    ),
    source as (
        select
            _id,

            rota,
            nome,
            descricao,

            safe_cast(createdat as datetime) as createdat,
            safe_cast(updatedat as datetime) as updatedat,

            __v,

            safe_cast(safe_cast(data_extracao as timestamp) as date) as data_extracao,
            safe_cast(ano_particao as int64) as ano_particao,
            safe_cast(mes_particao as int64) as mes_particao,
            safe_cast(data_particao as date) as data_particao
        from {{ source("brutos_minhasaude_mongodb_staging", "modulos_perfil_acessos") }}
        where data_extracao = (select max_partition from last_partition)
    )

select distinct *
from source
