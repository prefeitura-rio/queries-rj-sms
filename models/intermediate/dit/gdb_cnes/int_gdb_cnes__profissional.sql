{{
    config(
        alias="profissional",
        materialized="table",
        tags=["gdb_cnes"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}


with 
    profissional as (
        select * from {{ ref("raw_gdb_cnes__profissional") }}
        where data_particao = (select max(data_particao) from {{ ref("raw_gdb_cnes__profissional") }})
    )

select *
from profissional