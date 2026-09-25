{{
  config(
    schema="exemplo",
    alias="tabela_show",
    materialized="table",
  )
}}


select
  "olá galerinha!" as coluna1,
  "tudo bom?" as coluna2
