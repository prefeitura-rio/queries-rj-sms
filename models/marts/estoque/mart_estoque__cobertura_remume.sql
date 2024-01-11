{{
    config(
        alias="estoque_cobertura_remume",
        schema="projeto_estoque",
        materialized="table",

    )
}}


-- - Calcula para cada material quantas unidades de saúde de estoque positivo
with
    -- - source
    estoque as (select * from {{ ref("mart_estoque__posicao_atual") }}),

    estoque_sumarizado as (
        select
            id_material,
            material_descricao,
            id_cnes,
            sum(material_quantidade) as material_quantidade
        from estoque
        where
            material_remume = 'sim'
            and estabelecimento_tipo_sms
            in ('CLINICA DA FAMILIA', 'CENTRO MUNICIPAL DE SAUDE', 'ESTOQUE CENTRAL')
        group by id_material, material_descricao, id_cnes
    ),

    -- - target
    final as (
        select
            id_material,
            material_descricao,
            count(distinct id_cnes) as estabelecimentos_contagem,
            count(distinct case when material_quantidade > 0 then id_cnes end) as estabelecimentos_estoque_positivo,
            sum(material_quantidade) as material_quantidade,
        from estoque_sumarizado
        group by id_material, material_descricao
        order by estabelecimentos_estoque_positivo, material_descricao
    )

select *
from final
where estabelecimentos_contagem > 1
