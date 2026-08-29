{{
    config(
        alias="pacientes_equipes",
        schema="saude_historico_clinico",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with source as (
    select
        *,
        safe_cast(cpf_paciente as int64) as cpf_particao
    -- from \\ ref("int_historico_clinico__vinculo_equipe_familia") //
)

select *
from source
