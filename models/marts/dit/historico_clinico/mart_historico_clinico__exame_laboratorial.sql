{{
    config(
        schema="saude_historico_clinico",
        alias="exame_laboratorial",
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
        select * from {{ ref("int_historico_clinico__exame__cientificalab") }}
    ),

    exames as (
        select
            paciente_cpf,
            exame_nome,
            codigo_apoio as exame_codigo,
            medico_solicitante,
            unidade_nome,
            laudo_url,
            data_assinatura as exame_data_assinatura,
            safe_cast(paciente_cpf as int64) as cpf_particao
        from source
    )

select
    unidade_nome,
    paciente_cpf,
    exame_nome,
    exame_codigo,
    exame_data_assinatura,
    laudo_url,
    medico_solicitante,
    cpf_particao
from exames
