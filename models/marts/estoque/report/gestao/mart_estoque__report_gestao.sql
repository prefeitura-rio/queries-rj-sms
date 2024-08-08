{{
    config(
        alias="report_gestao",
        schema="projeto_estoque",
        materialized="table",
        tag=["report", "weekly"],
    )
}}


with
    -- - sources
    medicamentos as (
        select *
        from {{ ref("dim_material") }}
        where remume_indicador = 'sim' and remume_listagem_basico_indicador = "sim"
    ),  # TODO: revisar critério de seleção de medicamentos

    posicao as (
        select *
        from {{ ref("mart_estoque__posicao_atual_agregado") }}
        inner join medicamentos using (id_material)
        where estabelecimento_tipo_sms_agrupado in ("APS", "TPC")
    ),

    posicao_aps as (
        select *
        from posicao
        where estabelecimento_tipo_sms_agrupado = "APS"
    ),

    -- - transformations
    -- dias de estoque
    cmd as (
        select id_material, sum(material_consumo_medio) as cmd
        from posicao_aps
        group by id_material
    ),

    posicao_pivoted as (
        select
            p.id_material,
            coalesce(p.aps, 0) as qtd_aps,
            coalesce(p.tpc, 0) as qtd_tpc,
            cmd.cmd,
        from
            (
                select
                    id_material, estabelecimento_tipo_sms_agrupado, material_quantidade
                from posicao
            ) pivot (
                sum(material_quantidade) for estabelecimento_tipo_sms_agrupado
                in ("APS", "TPC")
            ) as p
        left join cmd using (id_material)
    ),

    -- zeradas
    -- - unidades zeradas
    ubs_zeradas as (
        select
            id_material,
            count(
                distinct case when material_quantidade = 0 then id_cnes else null end
            ) as zerados_ubs,
        from posicao_aps
        group by id_material
        order by zerados_ubs
    ),

    -- - areas programáticas zeradas
    ap_consolidado as (
        select
            id_material,
            estabelecimento_area_programatica as ap,
            sum(material_quantidade) as qtd,
            sum(material_consumo_medio) as cmd,
            {{
                dbt_utils.safe_divide(
                    "sum(material_quantidade)", "sum(material_consumo_medio)"
                )
            }} as cobertura,
        from posicao_aps
        group by id_material, estabelecimento_area_programatica
        order by id_material, estabelecimento_area_programatica
    ),

    ap_zeradas as (
        select
            id_material,
            count(distinct case when qtd < 1 then ap else null end) as zeradas_ap,
        from ap_consolidado
        group by id_material
    ),

    -- - final query
    final as (
        select
            m.id_material,
            m.nome,
            m.hierarquia_n1_categoria,
            m.hierarquia_n2_subcategoria,
            p.qtd_aps,
            p.qtd_tpc,
            p.cmd,
            round(
                {{ dbt_utils.safe_divide("p.qtd_aps", "p.cmd") }}, 2
            ) as cobertura_aps,
            round(
                {{ dbt_utils.safe_divide("p.qtd_tpc", "p.cmd") }}, 2
            ) as cobertura_tpc,
            round(
                {{ dbt_utils.safe_divide("p.qtd_aps + p.qtd_tpc", "p.cmd") }}, 2
            ) as cobertura_total,
            zu.zerados_ubs,
            za.zeradas_ap,
        from medicamentos as m
        left join posicao_pivoted as p using (id_material)
        left join ubs_zeradas as zu using (id_material)
        left join ap_zeradas as za using (id_material)
        order by cobertura_total asc
    )

select *
from final