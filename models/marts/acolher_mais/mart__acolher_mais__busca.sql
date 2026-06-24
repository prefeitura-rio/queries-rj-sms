{{
    config(
        alias="busca",
        materialized="table",
        cluster_by="nome",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    pacientes_busca as (
        select
            *
        from {{ ref('mart_historico_clinico_app__busca') }}
    )
select *
from pacientes_busca
