{{
    config(
        alias="estoque_posicao_atual_agregado_acesso_nivel_registro_para_looker",
        schema="projeto_estoque",
        materialized="table",
    )
}}


with
    estoque as (select * from {{ ref("mart_estoque__posicao_atual_agregado") }}),
    
    acessos as (select * from {{ source("brutos_sheets", "projeto_estoque_acessos") }}),

    estoque_com_acessos as (
        select estoque.*, acessos.email,
        from estoque
        inner join acessos on estoque.id_cnes = acessos.id_cnes
    )

select *
from estoque_com_acessos
