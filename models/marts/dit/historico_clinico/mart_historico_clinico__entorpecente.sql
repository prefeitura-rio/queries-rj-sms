{{
    config(
        schema="saude_historico_clinico",
        alias="entorpecente",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

with 
    drogas_tb as (
        select
            cpf as paciente_cpf,
            droga,
            safe_cast(cpf as int64) as cpf_particao
        from {{ ref("int_historico_clinico__drogas__pcsm") }}
    )

select 
    paciente_cpf,
    droga as entorpecentes,
    struct(current_timestamp() as processed_at) as metadados,
    cpf_particao
from drogas_tb