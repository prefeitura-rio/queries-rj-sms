{{
    config(
        schema="prep_estoque",
    )
}}

with
    consumo as (
        select
            estabelecimento.tipo,
            estabelecimento.area_programatica,
            estoque.id_cnes,
            estoque.id_material,
            estoque.material_valor_total
        from {{ ref("fct_estoque_movimento") }} as estoque
        left join {{ ref("dim_estabelecimento") }} using (id_cnes)
        where estoque.movimento_tipo_grupo = 'Consumo' and estoque.material_valor_total > 0
    ),
    atencao_primaria as (
        select
            concat("ap", "-", area_programatica, "-", id_material) as id_curva_abc,
            concat("ap", "-", area_programatica) as abc_unidade_agrupadora,  -- Na atenção primária o ABC é calculado por AP
            id_material,
            sum(material_valor_total) as material_valor_total
        from consumo
        where tipo = "CENTRO DE SAUDE/UNIDADE BASICA"
        group by id_curva_abc, abc_unidade_agrupadora, id_material
    ),
    atencao_especializada as (
        select
            concat("cnes", "-", id_cnes, "-", id_material) as id_curva_abc,
            concat("cnes", "-", id_cnes) as abc_unidade_agrupadora,  -- Nas demais unidades é agrupado pela própria unidade somente
            id_material,
            sum(material_valor_total) as material_valor_total
        from consumo
        where tipo <> "CENTRO DE SAUDE/UNIDADE BASICA"
        group by id_curva_abc, abc_unidade_agrupadora, id_material
    )

select *
from atencao_primaria
union all
select *
from atencao_especializada
