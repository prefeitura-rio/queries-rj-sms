{{
    config(
        alias="cadastro",
        schema="projeto_ipp",
        materialized="table",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with cadastro as (
    select distinct
        nome,
        mae_nome,
        data_nascimento,
        cpf
    from {{ ref('raw_prontuario_vitacare__paciente') }}
)

select      
    cpf,
    nome,
    data_nascimento,        
    mae_nome,
    safe_cast(cpf as int64) as cpf_particao
from cadastro
