{{
    config(
        schema="intermediario_gdb_sih",
        alias="haih_status",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select
            *
        from {{ ref("raw_gdb_sih__haih_status") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__haih_status") }}
        )
        qualify row_number() over (
            partition by xxxxx  -- FIXME
            order by data_carga desc
        ) = 1
    )

select *
from dedup
