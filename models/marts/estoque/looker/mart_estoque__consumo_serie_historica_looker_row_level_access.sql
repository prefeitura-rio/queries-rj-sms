{{
    config(
        alias="estoque_consumo_serie_historica_nivel_registro_para_looker",
        schema="projeto_estoque",
        materialized="table",
        cluster_by="email",
    )
}}


with
    consumo as (select * from {{ ref("mart_estoque__consumo_serie_historica") }}),
    
    acessos as (select id_cnes, email from {{ ref('gerenciamento_acessos__looker_farmacia') }}),

    consumo_com_acessos as (
        select consumo.*, acessos.email,
        from consumo
        inner join acessos on consumo.id_cnes = acessos.id_cnes
    )

select *
from consumo_com_acessos
