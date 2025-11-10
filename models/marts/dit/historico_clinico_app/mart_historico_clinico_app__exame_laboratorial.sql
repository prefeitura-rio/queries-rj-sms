{{
    config(
        alias="exame_laboratorial",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with exame_laboratorial as (
    select 
        * 
    from {{ ref('mart_historico_clinico__exame_laboratorial') }}
)

select * from exame_laboratorial