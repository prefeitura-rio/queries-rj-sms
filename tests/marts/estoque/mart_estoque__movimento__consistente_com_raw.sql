-- verifica se os valores consolidados nos dados mestres de movimento batem com os
-- valores
-- encontrados que estão chegando da tpc, vitai e vitacare
with

    -- SOURCES
    estabelecimento as (
        select
            *
        from {{ ref("dim_estabelecimento") }}
    ),

    raw_vitai as (
        select
            id_cnes,
            data_particao,
            'vitai' as sistema_origem,
            sum(material_valor_total) as raw__material_valor_total,
            sum(material_quantidade) as raw__material_quantidade
        from {{ ref("raw_prontuario_vitai__estoque_movimento") }}
        group by 1, 2
    ),

    raw_vitacare as (
        select
            id_cnes,
            data_particao,
            'vitacare' as sistema_origem,
            sum(0) as raw__material_valor_total,
            sum(material_quantidade) as raw__material_quantidade,
        from {{ ref("raw_prontuario_vitacare__estoque_movimento") }}
        group by 1, 2
    ),

    raw_union as (
        select *
        from raw_vitai
        union all
        select *
        from raw_vitacare
    ),

    -- MART
    mart_movimento as (
        select
            id_cnes,
            data_particao,
            sum(material_valor_total) as mart__material_valor_total,
            sum(material_quantidade) as mart__material_quantidade
        from {{ ref("mart_estoque__movimento") }}
        group by 1, 2
    ),

    resultado as (
        select
            raw.id_cnes,
            est.nome_limpo,
            est.area_programatica,
            raw.* except (id_cnes),
            mart.mart__material_quantidade,
            mart.mart__material_valor_total
        from raw_union as raw
        left join
            mart_movimento as mart
            on raw.id_cnes = mart.id_cnes
            and raw.data_particao = mart.data_particao
        left join estabelecimento as est on raw.id_cnes = est.id_cnes
    )

select *
from resultado
where
    abs(mart__material_quantidade - raw__material_quantidade) > 0.1
    or (abs(mart__material_valor_total - raw__material_valor_total) > 0.1 and sistema_origem = 'vitai')
    -- adicionada uma tolerancia de .1 para evitar falsos positivos devido ao
    -- truncamento de casas decimais
    
