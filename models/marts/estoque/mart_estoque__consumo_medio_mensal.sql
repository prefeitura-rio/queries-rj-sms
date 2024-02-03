{{
    config(
        alias="estoque_consumo_medio_mensal",
        schema="projeto_estoque",
        materialized="table",
    )
}}

with cmm as (select * from {{ ref("int_estoque__dispensacao_media_mensal") }})

select
    id_cnes,
    id_material,
    quantidade as consumo_medio_diario,
    quantidade * 30 as consumo_medio_mensal
from cmm

