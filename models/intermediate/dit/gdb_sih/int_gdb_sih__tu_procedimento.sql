{{
    config(
        schema="intermediario_gdb_sih",
        alias="tu_procedimento",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select
            *

        from {{ ref("raw_gdb_sih__tu_procedimento") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__tu_procedimento") }}
        )
        qualify row_number() over (
            partition by codigo_procedimento
            order by data_carga desc
        ) = 1
    )

select *
from dedup
