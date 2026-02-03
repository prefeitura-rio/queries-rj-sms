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

    cr.source_id,
    cr.id_hci,

    cr.estabelecimento.nome as estabelecimento,

    cr.profissional.nome as profissional_nome,
    cr.profissional.cbo as profissional_cbo, -- TODO: cargo

    cr.contrarreferencia.numero as documento_numero,
    cr.contrarreferencia.datahora as documento_datahora,

    concat(
      cr.avaliacao.conduta,
      "\n---------------\n",
      cr.avaliacao.seguimento
    ) as conduta,
    cr.avaliacao.resumo as resumo,

    safe_cast(cr.paciente.cpf as int64) as cpf_particao

  from {{ ref("mart_historico_clinico__contrarreferencia") }} as cr
)

select *
from source
