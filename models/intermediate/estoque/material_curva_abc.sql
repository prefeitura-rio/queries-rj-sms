with
    inventory as (
        select * from {{ref("dispensacao_agrupada_por_unidade_abc_e_material")}}  -- curva ABC é calculada a partir dos materiais com maior valor total dispensados
        where material_valor_total > 0
    ),
    total as (
        select abc_unidade_agrupadora, sum(material_valor_total) as total_value from inventory group by abc_unidade_agrupadora
    ),
    cumulative as (
        select
            abc_unidade_agrupadora,
            id_material,
            id_curva_abc,
            material_valor_total,
            sum(material_valor_total) over (
                partition by abc_unidade_agrupadora order by material_valor_total desc
            ) as cumulative_value
        from inventory
    ),

    abc_analysis as (
        select
            cv.abc_unidade_agrupadora,
            cv.id_material,
            cv.id_curva_abc,
            cv.material_valor_total,
            cv.cumulative_value,
            (cv.cumulative_value / tv.total_value) * 100 as cumulative_percentage
        from cumulative cv
        join total tv on cv.abc_unidade_agrupadora = tv.abc_unidade_agrupadora
    )
select
    id_curva_abc,
    abc_unidade_agrupadora,
    id_material,
    material_valor_total,
    cumulative_value as valor_acumulado,
    cumulative_percentage as valor_acumulado_percentual,
    case
        when cumulative_percentage <= 80
        then 'A'
        when cumulative_percentage > 80 and cumulative_percentage <= 95
        then 'B'
        else 'C'
    end as abc_categoria
from abc_analysis
order by abc_unidade_agrupadora, cumulative_percentage asc
