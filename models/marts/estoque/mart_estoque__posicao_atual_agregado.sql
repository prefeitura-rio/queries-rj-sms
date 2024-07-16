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
            estabelecimento_tipo_sms_agrupado,
            material_hierarquia_n1_categoria,
            material_hierarquia_n2_subcategoria,
            material_controlado_indicador,
            material_controlado_tipo,
            material_descricao,
            material_remume_indicador,
            busca_material_id_descricao_case_insensitive,
            sum(material_quantidade) as material_quantidade,
            avg(material_consumo_medio) as material_consumo_medio
        from source
        where lote_validade_dentro_indicador = "sim"
        group by
            id_cnes,
            id_material,
            estabelecimento_area_programatica,
            estabelecimento_nome_limpo,
            estabelecimento_tipo_sms_agrupado,
            material_hierarquia_n1_categoria,
            material_hierarquia_n2_subcategoria,
            material_controlado_indicador,
            material_controlado_tipo,
            material_remume_indicador,
            material_descricao,
            busca_material_id_descricao_case_insensitive
    )

select
    *,
    {{ dbt_utils.safe_divide("material_quantidade", "material_consumo_medio") }}
    as estoque_cobertura_dias
from agregado
