{{
    config(
        schema="intermediario_gdb_cnes",
        alias="equipe",
        materialized="table",
        tags=["gdb_cnes"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}


with
    base as (
        select *
        from {{ ref("raw_gdb_cnes__equipe") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__equipe") }}
        )
        qualify row_number() over (
            partition by id_unidade, equipe_ine
            order by data_carga desc
        ) = 1
    ),
    tipo as (
        select *
        from {{ ref("raw_gdb_cnes__equipe_tipo") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__equipe_tipo") }}
        )
        qualify row_number() over (
            partition by id_equipe_tipo
            order by data_carga desc
        ) = 1
    ),

    joined as (
        select
            id_equipe_tipo,
            tipo.equipe_descricao,
            tipo.id_equipe_grupo,
            base.* except (id_equipe_tipo)
        from base
        left join tipo
            using (id_equipe_tipo)
    )

select *
from joined
