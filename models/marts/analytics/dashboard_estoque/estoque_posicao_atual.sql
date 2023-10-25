{{
    config(
        schema="dashboard_estoque",
        materialized="view",
    )
}}


with
    posicao_atual as (
        select * from {{ ref("estoque_posicao") }} where data_particao = current_date()
    ),
    historico_dispensacao as (
        select * from {{ ref("dispensacao_agrupada_por_unidade_abc_e_material") }}
    ),
    curva_abc as (select * from {{ ref("material_curva_abc") }}),
    dispensacao_media as (select * from {{ ref("dispensacao_media_mensal") }}),
    material as (select * from {{ ref("material") }}),
    posicao_final as (
        select
            pos.*,
            cmm.quantidade as material_consumo_medio,
            coalesce(abc.abc_categoria, "S/C") as abc_categoria,
            coalesce(mat.nome, pos.material_descricao) as material_descricao2,
            if(mat.nome is null, "nao", "sim") as material_cadastro_correto,
            case
                when abc.abc_categoria is not null
                then '-'
                when abc.abc_categoria is null and sistema_origem = 'tpc'
                then "ABC não calculado para TPC"
                when abc.abc_categoria is null and pos.material_valor_total = 0
                then 'item não possui preço cadastrado'
                when abc.abc_categoria is null and disp.id_curva_abc is null
                then "item não possui histórico de dispensação registrado na unidade/região"
                else 'desconhecida'
            end as abc_justificativa_ausencia,
            case
                when cmm.quantidade is not null
                then '-'
                when cmm.quantidade is null and sistema_origem = 'tpc'
                then "CMM não calculado para TPC"
                when cmm.quantidade is null and disp.id_curva_abc is null
                then "item não possui histórico de dispensação registrado na unidade"
                else 'desconhecida'
            end as cmm_justificativa_ausencia,
        from posicao_atual as pos
        left join curva_abc as abc using (id_curva_abc)
        left join historico_dispensacao as disp using (id_curva_abc)
        left join
            dispensacao_media as cmm
            on (pos.id_material = cmm.id_material and pos.id_cnes = cmm.id_cnes)
        left join material as mat on (pos.id_material = mat.id_material)
    )

select
    id_cnes,
    id_curva_abc,
    id_material,
    abc_categoria,
    material_descricao2 as material_descricao,
    material_unidade,
    material_cadastro_correto,
    estoque_secao,
    id_lote,
    lote_data_vencimento,
    material_quantidade,
    material_valor_total,
    material_consumo_medio,
    {{ dbt_utils.safe_divide("material_quantidade", "material_consumo_medio") }}
    as estoque_cobertura_dias,
    abc_justificativa_ausencia,
    cmm_justificativa_ausencia,
    sistema_origem,
    data_particao,
    data_carga
from posicao_final
