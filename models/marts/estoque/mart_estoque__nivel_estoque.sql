{{
    config(
        alias="estoque_nivel_estoque",
        schema="projeto_estoque",
        materialized="table",
    )
}}

with
    -- - Source
    estoque as (select * from {{ ref("fct_estoque_posicao") }}),

    estabelecimento as (select * from {{ ref("dim_estabelecimento") }}),

    -- - Transform
    daily_total_stock as (
        select data_particao, id_cnes, sum(material_valor_total) as material_valor_total
        from estoque
        group by data_particao, id_cnes
    ),

    weekly_average_stock as (
        select
            id_cnes,
            extract(week from data_particao) as data_particao,
            avg(material_valor_total) as material_valor_total
        from daily_total_stock
        group by 1, 2
    ),

    final as (
        select
            stock.*,
            coalesce(estab.tipo_sms, 'TPC') as tipo_sms,
            coalesce(estab.tipo_sms_simplificado, 'TPC') as tipo_sms_simplificado,
            nome_limpo  -- -, estab.*
        from daily_total_stock as stock
        left join estabelecimento as estab on estab.id_cnes = stock.id_cnes
    )

select *
from final
