-- Relação de posições REMUME zeradas no Vitacare
with
    -- source
    posicao_mais_recente_por_estabelecimento as (
        select *
        from {{ ref("int_estoque__posicao_mais_recente_por_estabelecimento") }}
        where prontuario_versao = 'vitacare'
    ),

    posicao_atual as (
        select raw.*
        from {{ ref("raw_prontuario_vitacare__estoque_posicao") }} as raw
        inner join
            posicao_mais_recente_por_estabelecimento as recente
            on recente.id_cnes = raw.id_cnes
            and recente.data_particao = raw.data_particao
    ),

    materiais as (select * from {{ ref("dim_material") }}),

    -- relacão de unidades que posição de estoque na data atual
    unidades_vitacare as (select distinct id_cnes, data_particao from posicao_atual),

    -- relação de itens remume por estabelecimento
    remume as (
        select remume.*, est.data_particao
        from
            {{ ref("int_estoque__material_relacao_remume_por_estabelecimento") }}
            as remume
        inner join unidades_vitacare as est on remume.id_cnes = est.id_cnes
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
            "" as id_estoque_posicao,
            "" as area_programatica,
            pz.id_cnes,
            "" as id_lote,
            pz.id_material,
            "" as id_atc,
            "" as estabelecimento_nome,
            cast(null as date) as lote_data_cadastro,
            cast(null as date) as lote_data_vencimento,
            mat.nome as material_descricao,
            pz.material_quantidade,
            pz.data_particao,
            pz.data_particao as data_carga,
        from posicao_zeradas as pz
        left join materiais as mat on pz.id_material = mat.id_material
    )

select *
from final