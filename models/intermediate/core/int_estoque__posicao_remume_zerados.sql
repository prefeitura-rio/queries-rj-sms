-- Verifica a relação de materiais contidos na remume que tem a sua posicao zerada
with

    -- Sources

    posicao_positiva as (select * from {{ ref("int_estoque__posicao_union_origens") }}),

    remume as (
        select *
        from {{ ref("int_estoque__material_relacao_remume_por_estabelecimento") }}
    ),

    -- Obtém os itens zerados
    posicao_zeradas as (
        select remume.id_material, remume.id_cnes, 0 as material_quantidade,
        from remume
        left join posicao_positiva on remume.id_material = posicao_positiva.id_material
        where posicao_positiva.id_material is null
    )

-- Adiciona a mesma esrtutura da poisção positiva
select
    pos.id_cnes,
    "" as id_lote,
    pos.id_material,
    "" as estoque_secao,
    mat.nome as material_descricao,
    mat.unidade,
    "" as lote_data_vencimento,
    pos.material_quantidade,
    0 as material_valor_unitario,
    0 as material_valor_total,
from posicao_zeradas as pos
left join {{ ref("dim_material") }} as mat using (id_material)
