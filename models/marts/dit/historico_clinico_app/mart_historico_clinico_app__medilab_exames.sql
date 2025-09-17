{{
    config(
        alias="medilab_exames",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with medilab_exames as (
    select 
        * 
    from {{ ref('mart_historico_clinico__medilab_exames') }}
)

select * from medilab_exames