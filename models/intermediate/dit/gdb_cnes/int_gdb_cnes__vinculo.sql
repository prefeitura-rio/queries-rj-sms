{{
    config(
        schema = 'intermediario_gdb_cnes',
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
        select * from {{ ref("raw_gdb_cnes__vinculo") }}
        where data_particao = (select max(data_particao) from {{ ref("raw_gdb_cnes__vinculo") }})
    )

select *
from vinculo