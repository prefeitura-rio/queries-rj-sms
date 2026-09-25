{{
  config(
    schema="exemplo",
    alias="populacao_por_uf",
    materialized="table",
  )
}}


with
  source as (
    select
      sigla_uf,
      ano,
      sum(populacao) as populacao
    from {{ source("basedosdados_ibge_pop", "municipio") }}
    group by 1, 2
  ),

  dedup as (
    select
      sigla_uf,
      populacao
    from source
    -- Tabela possui população histórica, queremos somente a mais recente
    qualify row_number() over (
      partition by sigla_uf
      order by ano desc
    ) = 1
  )

select *
from dedup
order by sigla_uf
