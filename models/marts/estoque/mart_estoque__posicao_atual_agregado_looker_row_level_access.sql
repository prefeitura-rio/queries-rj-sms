{{
    config(
        alias="estoque_posicao_atual_agregado_acesso_nivel_registro_para_looker",
        schema="projeto_estoque",
        materialized="table",
        cluster_by="email",
    )
}}


with
    estoque as (select * from {{ ref("mart_estoque__posicao_atual_agregado") }}),
    
    acessos as (select id_cnes, email from {{ ref('gerenciamento_acessos__looker_farmacia') }}),

    estoque_com_acessos as (
        select estoque.*, acessos.email,
        from estoque
        inner join acessos on estoque.id_cnes = acessos.id_cnes
    )

select *
from estoque_com_acessos
