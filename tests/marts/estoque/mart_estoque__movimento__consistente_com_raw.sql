-- verifica se os valores consolidados no mart de posicao atual batem com os valores
-- encontrados que estão chegando da tpc, vitai e vitacare

with

    -- SOURCES
    source_vitacare as (
        select *, concat(id_cnes, "-", data_particao) as id_estabelecimento_particao
        from {{ ref("raw_prontuario_vitacare__estoque_movimento") }}
    ),

    source_vitai as (
        select *, concat(id_cnes, "-", data_particao) as id_estabelecimento_particao
        from {{ ref("raw_prontuario_vitai__estoque_movimento") }}
    ),

    -- MART
    mart_movimento as (
        select
            sistema_origem,
            sum(material_valor_total) as material_valor_total,
            sum(material_quantidade) as material_quantidade
        from {{ ref("mart_estoque__movimento") }}
        group by 1
    ),

    -- VITAI
    vitai as (
        select
            'vitai' as sistema_origem,
            sum(material_valor_total) as material_valor_total,
            sum(material_quantidade) as material_quantidade
        from source_vitai
        group by 1
    ),

    mart_vitai as (
        select
            mart.sistema_origem,
            mart.material_quantidade as mart__material_quantidade,
            origem.material_quantidade as origem__material_quantidade,
            mart.material_valor_total as mart__material_valor_total,
            origem.material_valor_total as origem__material_valor_total,
        from mart_movimento as mart
        left join vitai as origem on mart.sistema_origem = origem.sistema_origem
        where origem.sistema_origem is not null
    ),

    -- VITACARE
    -- Para vitacare, não temos o valor total, apenas a quantidade.
    -- valores vem da base da TPC. Não faz sentido utilizar neste teste
    vitacare as (
        select
            'vitacare' as sistema_origem,
            sum(material_quantidade) as material_quantidade
        from source_vitacare
        group by 1
    ),

    mart_vitacare as (
        select
            mart.sistema_origem,
            mart.material_quantidade as mart__material_quantidade,
            origem.material_quantidade as origem__material_quantidade,
            0 as mart__material_valor_total,
            0 as origem__material_valor_total,
        from mart_movimento as mart
        left join vitacare as origem on mart.sistema_origem = origem.sistema_origem
        where origem.sistema_origem is not null
    ),

    -- RESULTADO
    mart_consolidado as (
        select *
        from mart_vitai
        union all
        select *
        from mart_vitacare
    )

select *
from mart_consolidado
where
    abs(mart__material_quantidade - origem__material_quantidade) > 0.1
    or abs(mart__material_valor_total - origem__material_valor_total) > 0.1
    -- adicionada uma tolerancia de .1 para evitar falsos positivos devido ao truncamento de casas decimais
