-- Relação de posições REMUME zeradas no TPC

with
    particao_mais_recente as (
        select max(data_particao) as data_particao
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
    ),

    -- source
    posicao_atual as (
        select *
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
        where data_particao = (select data_particao from particao_mais_recente)

    ),
    materiais as (select * from {{ ref("dim_material") }}),

    -- relação de itens remume por estabelecimento
    remume as (
        select *
        from {{ ref("int_estoque__material_relacao_remume_por_estabelecimento") }}
        where tipo_sms_simplificado = "TPC"
    ),

    -- materias em estoque
    materiais_com_estoque as (select distinct id_material from posicao_atual),

    -- Filtra as posições zeradas
    posicao_zeradas as (
        select remume.id_material, remume.id_cnes, 0 as material_quantidade,
        from remume
        left join
            materiais_com_estoque as em_estoque
            on remume.id_material = em_estoque.id_material
        where em_estoque.id_material is null
    ),

    -- Transforma as posições zeradas na mesma estrutura da posição atual
    posicao_zeradas_estruturada as (
        select
            "" as id_lote,
            pz.id_material,
            "nao" as estoque_reservado_para_abastecimento,
            "" as estoque_secao,
            mat.nome as material_descricao,
            "" as material_unidade,
            cast(null as date) as lote_data_vencimento,
            pz.material_quantidade,
            0 as material_valor_unitario,
            0 as material_valor_total,
            (select data_particao from particao_mais_recente) as data_particao, 
            current_datetime() as data_snapshot,
            current_datetime() as data_carga,
        from posicao_zeradas as pz
        left join materiais as mat on pz.id_material = mat.id_material
    )

-- Une os itens zerados com a posição atual
select *
from posicao_zeradas_estruturada