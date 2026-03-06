{{
    config(
        alias="int_gdb_cnes__profissional",
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
        qualify row_number() over (partition by id_profissional_cnes order by data_carga desc) = 1
    )

select *
from profissional