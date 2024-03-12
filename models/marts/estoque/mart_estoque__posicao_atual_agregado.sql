{{
    config(
        alias="estoque_posicao_atual_agregado",
        schema="projeto_estoque",
        materialized="table",
    )
}}

with
    source as (select * from {{ ref("mart_estoque__posicao_atual") }}),

    agregado as (
        select
            id_cnes,
            id_material,
            estabelecimento_area_programatica,
            estabelecimento_nome_limpo,
            estabelecimento_agrupador_sms,
            material_natureza,
            material_hierarquia_subclasse,
            material_descricao,
            material_remume,
            sum(material_quantidade) as material_quantidade,
            avg(material_consumo_medio) as material_consumo_medio
        from source
        where lote_status = "ativo"
        group by
            id_cnes,
            id_material,
            estabelecimento_area_programatica,
            estabelecimento_nome_limpo,
            estabelecimento_agrupador_sms,
            material_natureza,
            material_hierarquia_subclasse,
            material_remume,
            material_descricao
    )

select
    *,
    {{ dbt_utils.safe_divide("material_quantidade", "material_consumo_medio") }}
    as estoque_cobertura_dias
from agregado
