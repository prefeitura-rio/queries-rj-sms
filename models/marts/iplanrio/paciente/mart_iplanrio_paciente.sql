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
),

-- Para diminuir o custo, a varredura tem como base os episodios da camada intermediaria da vitacare, ao inves do mart historico clinico
-- Essa variavel é usada no RMI e em um dos protocolo do PIC
atendimento as (
    select distinct
        cpf
    from {{ ref("int_historico_clinico__episodio__vitacare") }} 
    where cpf is not null
)

select
    base.cpf,
    base.cns,

    struct(
        base.dados.obito_data      as obito_data,
        base.dados.obito_indicador as obito_indicador,
        base.dados.raca            as raca
    ) as dados,

    base.equipe_saude_familia,

    struct(
        base.contato.email    as email,
        base.contato.telefone as telefone
    ) as contato,

    base.endereco,

    coalesce(atendimento.cpf is not null, false) as tem_atendimento_aps,

    base.cpf_particao,

    struct(
        current_timestamp() as ultima_atualizacao
    ) as metadados

from base
left join atendimento
    on base.cpf = atendimento.cpf