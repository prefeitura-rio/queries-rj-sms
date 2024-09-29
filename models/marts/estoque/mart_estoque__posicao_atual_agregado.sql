{{
    config(
        alias="estoque_posicao_atual_agregado",
        schema="projeto_estoque",
        materialized="table",
    )
}}

with
    source as (
        select *
        from {{ ref("mart_estoque__posicao_atual") }}
        where lote_status_padronizado in ("Ativo")
        and not (sistema_origem = "vitacare" and estoque_secao_caf_indicador = "Não") -- dados da vitacare fora do estoque central não são confiáveis
    ),

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
            max(data_particao) as data_particao,
            sum(material_quantidade) as material_quantidade,
            avg(material_consumo_medio) as material_consumo_medio
        from source
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
    ),

    final as (
        select
            *,
            {{ dbt_utils.safe_divide("material_quantidade", "material_consumo_medio") }}
            as estoque_cobertura_dias
        from agregado
    )

select *
from final
