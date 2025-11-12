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
    ),

    exames_agg as (
        select
            paciente_cpf,
            array_agg(exame_nome) as exame_nome,
            array_agg(exame_codigo) as exame_codigo,
            array_agg(exame_data_assinatura) as exame_data_assinatura,
            medico_solicitante,
            unidade_nome,
            laudo_url,
            cpf_particao
        from exames
        group by
            paciente_cpf,
            medico_solicitante,
            unidade_nome,
            laudo_url,
            cpf_particao
    )

select
    *
from exames_agg
