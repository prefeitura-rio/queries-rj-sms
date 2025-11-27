{{
  config(
    alias="bairro",
    materialized="table"
  )
}}

with src as (
  select *
  from {{ source("datario_dados_mestres", "bairro") }}
)
select *
from src
