{{
    config(
        schema="saude_historico_clinico",
        alias="comorbidade",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
    )
}}

with
    comorbidade_tb as (
        select
          cpf as paciente_cpf,
          comorbidade,
          safe_cast(cpf as int64) as cpf_particao
        from {{ ref("int_historico_clinico__comorbidade__pcsm") }}
    )

select 
    paciente_cpf,
    comorbidade as comorbidades,
    struct(current_timestamp() as processed_at) as metadados,
    cpf_particao
from comorbidade_tb