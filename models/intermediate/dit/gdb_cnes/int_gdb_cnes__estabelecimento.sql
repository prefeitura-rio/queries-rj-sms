{{
    config(
        alias="estabelecimento",
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
    estabelecimento as (
        select * from {{ ref("raw_gdb_cnes__estabelecimento") }}
        where data_particao = (select max(data_particao) from {{ ref("raw_gdb_cnes__estabelecimento") }})
    )

select * from estabelecimento

