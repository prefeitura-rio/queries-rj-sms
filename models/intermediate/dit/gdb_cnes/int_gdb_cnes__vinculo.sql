{{
    config(
        schema="intermediario_gdb_cnes",
        alias="vinculo",
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
    vinculo as (
        select *
        from {{ ref("raw_gdb_cnes__vinculo") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__vinculo") }}
        )
        -- Às vezes temos múltiplos GDBs pra uma mesma competência,
        -- porque são revisados etc; então precisamos deduplicar
        qualify row_number() over (
            partition by id_profissional_sus, id_unidade, id_cbo
            order by data_carga desc
        ) = 1
    )

select *
from vinculo
