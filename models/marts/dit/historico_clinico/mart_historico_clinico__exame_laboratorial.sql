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
            codigo_apoio,
            medico_solicitante,
            unidade_nome,
            laudo_url,
            data_assinatura,
            safe_cast(paciente_cpf as int64) as cpf_particao
        from source
    ),

    exames_agg as (
        select
            paciente_cpf,
            array_agg(
                struct(
                    exame_nome as descricao,
                    codigo_apoio as codigo,
                    data_assinatura
                )
            ) as exame,
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
