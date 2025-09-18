{{
    config(
        alias="cpf",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

select * from {{ source("brutos_bcadastro", 'cpf') }}
