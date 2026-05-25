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
        select *
        from {{ ref("int_historico_clinico__exames_laboratoriais") }}
    ),

    exames as (
        select
            id_solicitacao,
            paciente_cpf,
            exame_nome,
            codigo_apoio,
            medico_solicitante,
            unidade_nome,
            laudo_url,
            datahora_pedido,
            datahora_assinatura,
            safe_cast(paciente_cpf as int64) as cpf_particao
        from source
    ),

    exames_agg as (
        select
            paciente_cpf,
            id_solicitacao,
            array_agg(
                struct(
                    exame_nome as descricao,
                    codigo_apoio as codigo,
                    datahora_assinatura
                )
            ) as exames,
            medico_solicitante,
            unidade_nome,
            laudo_url,
            datahora_pedido,
            max(datahora_assinatura) as _ultima_datahora_assinatura,
            cpf_particao
        from exames
        group by
            paciente_cpf,
            id_solicitacao,
            medico_solicitante,
            unidade_nome,
            laudo_url,
            datahora_pedido,
            cpf_particao
    )

select
    *
from exames_agg
