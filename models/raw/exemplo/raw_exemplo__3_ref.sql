{{
  config(
    schema="exemplo",
    alias="municipios_por_uf",
    materialized="table",
  )
}}


select
  nome_uf as uf,
  count(distinct cod_mun) as qtd_municipios
from {{ ref("raw_sheets__municipios_brasil") }}
group by 1
order by 1
