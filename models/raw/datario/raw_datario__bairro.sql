{{
  config(
    alias="bairro",
    materialized="table",
    meta={"owner": "avellar"}
  )
}}

with src as (
  select *
  from {{ source("datario_dados_mestres", "bairro") }}
)
select *
from src
