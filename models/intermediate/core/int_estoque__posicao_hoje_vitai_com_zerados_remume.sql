-- posicao do dia de hoje adicionado os materiais remume que estão zerados

with
    -- source
    posicao_atual as (
        select *
        from {{ ref("raw_prontuario_vitai__estoque_posicao") }}
        where data_particao = current_date()
    ),

    materiais as (select * from {{ ref("dim_material") }}),

    -- relacão de unidades que posição de estoque na data atual
        unidades_vitai_com_posicao_atual as (
        select distinct id_cnes from posicao_atual
    ),

    -- relação de itens remume por estabelecimento
    remume as (
        select remume.*
        from
            {{ ref("int_estoque__material_relacao_remume_por_estabelecimento") }}
            as remume
        left join unidades_vitai_com_posicao_atual as est on remume.id_cnes = est.id_cnes
        where est.id_cnes is not null
    ),

    -- Filtra as posições zeradas
    posicao_zeradas as (
        select remume.id_material, remume.id_cnes, 0 as material_quantidade,
        from remume
        left join posicao_atual on remume.id_material = posicao_atual.id_material
        where posicao_atual.id_material is null
    ),

    -- Transforma as posições zeradas na mesma estrutura da posição atual
    posicao_zeradas_estruturada as (
        select
            pz.id_cnes,
            "" as id_lote,
            pz.id_material,
            "" as estoque_secao,
            mat.nome as material_descricao,
            mat.unidade as material_unidade,
            cast(null as date) as lote_data_vencimento,
            pz.material_quantidade,
            0 as material_valor_unitario,
            0 as material_valor_total,
            current_date() as data_particao,
            current_datetime() as data_snapshot,
            current_datetime() as data_carga,
        from posicao_zeradas as pz
        left join materiais as mat on pz.id_material = mat.id_material
    )

-- Une os itens zerados com a posição atual
select *
from posicao_atual
union all
select *
from posicao_zeradas_estruturada
where lote_data_vencimento is not null
