{{
    config(
        alias="pacientes_equipes",
        schema="app_historico_clinico",
        materialized="table",
        partition_by={
            "field": "ine_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 10000000000, "interval": 2500000},
        },
    )
}}

with source as (
    select
        id_ine,
        cpf_paciente,
        data_atualizacao_vinculo_equipe
    from {{ ref("mart_historico_clinico__pacientes_equipes") }}
    qualify row_number() over (
        partition by id_ine, cpf_paciente
        order by data_atualizacao_vinculo_equipe desc
    ) = 1
)

select
    *,
    safe_cast(id_ine as int64) as ine_particao
from source
