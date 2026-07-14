{{
  config(
    alias="episodios_mensais",
    schema="app_historico_clinico",
    materialized="table",
    partition_by={
      "field": "cpf_particao",
      "data_type": "int64",
      "range": {"start": 0, "end": 100000000000, "interval": 34722222},
    },
    cluster_by=['mes_referencia'],
  )
}}

with
  source as (
    select

      regexp_replace(
        cast(date_trunc(date(entry_datetime), month) as string),
        r"-01$",
        ""
      ) as mes_referencia,

      -- Não sei como seria possível ter mais de um tipo, até porque
      -- isso vem de uma planilha com somente 1 célula para tipo
      nullif(trim(filter_tags[safe_offset(0)]), "") as categoria,

      cpf_particao

    from {{ ref("mart_historico_clinico_app__episodio") }}
    where entry_datetime is not null
      and exibicao.indicador = true
  ),

  grouped as (
    select
      mes_referencia,

      case
        when categoria in (
          "POLICLINICA", "ESPECIALIDADE"
        )
          then "Especialidade"
        when categoria in (
          "HOSPITAL", "UPA", "CER"
        )
          then "Hospitalar"
        when categoria in (
          "MATERNIDADE", "ESPECIALIDADE",
          "PRISIONAL", "OUTROS"
        )
          then initcap(categoria)
        else coalesce(categoria, "Outros")
      end as categoria,

      count(*) as quantidade,
      cpf_particao
    from source
    group by 1, 2, 4
  )

select *
from grouped
