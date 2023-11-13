
with
    estoque as (
        select *, concat(id_material, "-", data_particao) as id_material_particao
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
    ),

    estoque_mais_recente_por_material as (
        select id_material, max(id_material_particao) as id_material_particao
        from estoque
        group by id_material
    ),

    estoque_mais_recente_por_material_com_preco as (
        select
            e.id_material,
            coalesce(e.material_valor_unitario, 0) as material_valor_unitario_corrigido,
            coalesce(e.material_valor_total, 0) as material_valor_total_corrigido,
            e.material_quantidade,
            e.data_particao
        from estoque as e
        left join
            estoque_mais_recente_por_material as e2
            on e.id_material_particao = e2.id_material_particao
        where e2.id_material is not null
    ),
    estoque_mais_rcecente_consolidado as (
        select
            id_material,
            sum(material_valor_total_corrigido) as material_valor_total,
            sum(material_quantidade) as material_quantidade,
            max(data_particao) as data_particao
        from estoque_mais_recente_por_material_com_preco
        group by id_material
    )

select
    id_material,
    material_quantidade,
    material_valor_total,
    if(
        material_valor_total = 0, 0, material_valor_total / material_quantidade
    ) as material_valor_unitario_medio,
    data_particao
from estoque_mais_rcecente_consolidado
