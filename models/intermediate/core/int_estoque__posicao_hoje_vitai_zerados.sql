-- posicao do dia de hoje adicionado os materiais remume que estão zerados
with
    -- source
    posicao_mais_recente_por_estabelecimento as (
        select *
        from {{ ref("int_estoque__posicao_mais_recente_por_estabelecimento") }}
        where prontuario_versao = 'vitai'
    ),

    posicao_atual as (
        select raw.*
        from {{ ref("raw_prontuario_vitai__estoque_posicao") }} as raw
        inner join
            posicao_mais_recente_por_estabelecimento as recente
            on recente.id_cnes = raw.id_cnes
            and recente.data_particao = raw.data_particao
    ),

    materiais as (select * from {{ ref("dim_material") }}),

    -- relacão de unidades que posição de estoque na data atual
    unidades_vitai as (select distinct id_cnes, data_particao from posicao_atual),

    -- relação de itens remume por estabelecimento
    remume as (
        select remume.*, est.data_particao
        from
            {{ ref("int_estoque__material_relacao_remume_por_estabelecimento") }}
            as remume
        inner join unidades_vitai as est on remume.id_cnes = est.id_cnes
    ),

    -- materias em estoque
    materiais_com_estoque as (select distinct id_cnes, id_material from posicao_atual),

    -- Filtra as posições zeradas
    posicao_zeradas as (
        select remume.id_material, remume.id_cnes, 0 as material_quantidade, remume.data_particao
        from remume
        left join
            materiais_com_estoque as em_estoque
            on remume.id_material = em_estoque.id_material
            and remume.id_cnes = em_estoque.id_cnes
        where em_estoque.id_material is null
    ),

    -- Transforma as posições zeradas na mesma estrutura da posição atual
    final as (
        select
            pz.id_cnes,
            "" as id_lote,
            pz.id_material,
            "" as estoque_secao,
            mat.nome as material_descricao,
            "" as material_unidade,
            cast(null as date) as lote_data_vencimento,
            pz.material_quantidade,
            0 as material_valor_unitario,
            0 as material_valor_total,
            pz.data_particao,
            current_datetime('America/Sao_Paulo') as data_snapshot,
            current_datetime('America/Sao_Paulo') as data_carga,
        from posicao_zeradas as pz
        left join materiais as mat on pz.id_material = mat.id_material
    )

-- Une os itens zerados com a posição atual
select *
from final