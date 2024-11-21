{{
    config(
        alias="funcionarios",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    source as (
        select
            *
        from {{ source("brutos_ergon_staging", "funcionarios") }}
    )
select *
from source
