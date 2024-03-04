{{
    config(
        alias="estoque_consumo_serie_historica_nivel_registro_para_looker",
        schema="projeto_estoque",
        materialized="table",
    )
}}


with
    consumo as (select * from {{ ref("mart_estoque__consumo_serie_historica") }}),
    
    acessos as (select * from {{ source("brutos_sheets", "projeto_estoque_acessos") }}),

    consumo_com_acessos as (
        select consumo.*, acessos.email,
        from consumo
        inner join acessos on consumo.id_cnes = acessos.id_cnes
    )

select *
from consumo_com_acessos
