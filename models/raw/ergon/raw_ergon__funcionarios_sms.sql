{{
    config(
        alias="funcionarios_sms",
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
            lpad(cpf, 11, '0') as cpf,
            * except(cpf, cpf_particao),
            cpf_particao
        from {{ source("brutos_ergon_staging", "funcionarios_sms") }}
    )
select *
from source
