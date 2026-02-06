{{
    config(
        schema="app_historico_clinico",
        alias="contrarreferencia",
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

    cr.id_hci,

    cr.estabelecimento.nome as estabelecimento,

    cr.profissional.nome as profissional_nome,
    cr.profissional.cargo as profissional_cargo,

    cr.contrarreferencia.numero as documento_numero,
    cr.contrarreferencia.datahora as documento_datahora,

    cr.avaliacao.conduta,
    cr.avaliacao.seguimento,
    cr.avaliacao.resumo,
    cr.avaliacao.historia_doenca_atual,
    cr.avaliacao.medicamentos_em_uso,
    cr.avaliacao.hipotese_diagnostica,

    safe_cast(cr.paciente.cpf as int64) as cpf_particao

  from {{ ref("mart_historico_clinico__contrarreferencia") }} as cr
)

select *
from source
