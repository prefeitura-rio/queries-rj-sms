{{
    config(
        alias="int_gdb_cnes__estabelecimento",
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
        qualify row_number() over (partition by id_unidade order by data_carga desc) = 1
    )

select * from estabelecimento

