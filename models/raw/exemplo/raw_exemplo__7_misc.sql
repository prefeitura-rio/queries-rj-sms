{{
  config(
    schema="exemplo",
    alias="ultimo_exemplo_iei",
    materialized="table",
    tags=["tag_de_exemplo"],
    meta={"owner": "avellar", "team": "cit"},
  )
}}
-- Tags de daily, monthly, ...

with
  source as (
    select
      "joao maria josé da silva" as nome1,
      "açúcar é uma delícia" as nome2,

      "" as campo1,
      "banana" as campo2,

      "2020-01-01" as data1,
      "2030-01-01" as data2
    from unnest([0])
  ),

  processado as (
    select

      {{ proper_br("nome1") }} as nome1,
      {{ remove_accents_upper("nome2") }} as nome2,

      cast({{ process_null("campo1") }} as string) as campo1,
      cast({{ process_null("campo2") }} as string) as campo2,

      {{ parse_and_filter_future_date("data1") }} as data1,
      {{ parse_and_filter_future_date("data2") }} as data2

    from source
  )


select *
from processado

-- Exemplos:
-- - GDB CNES
-- - is_incremental(): mart_cdi__email
-- - remove_html()
