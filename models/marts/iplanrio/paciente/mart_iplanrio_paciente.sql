{{
    config(
        schema="projeto_rmi",
        alias="paciente",
        materialized="table",
        tags=["hci", "paciente", "daily"],
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222}
        }
    )
}}

with base as (
    select *
    from {{ ref("mart_historico_clinico__paciente") }}
)

select
    cpf,
    cns,

    struct(
        base.dados.obito_data      as obito_data,
        base.dados.obito_indicador as obito_indicador,
        base.dados.raca            as raca
    ) as dados,

    equipe_saude_familia,

    struct(
        base.contato.email    as email,
        base.contato.telefone as telefone
    ) as contato,

    endereco,

    cpf_particao,

    struct(
        current_timestamp() as ultima_atualizacao
    ) as metadados

from base