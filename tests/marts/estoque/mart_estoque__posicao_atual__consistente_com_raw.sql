-- verifica se os valores consolidados no mart de posicao atual batem com os valores
-- encontrados que estão chegando da tpc, vitai e vitacare

with

    -- SOURCES
    -- filtra dados de 1 mês a fim de evitar full scan das tabelas.
    -- Premissa: Nenhuma ingestão deve estar 1 mês atrasada
    source_vitacare as (
        select *, concat(id_cnes, "-", data_particao) as id_estabelecimento_particao
        from {{ ref("raw_prontuario_vitacare__estoque_posicao") }}
        where data_particao > date_sub(current_date('America/Sao_Paulo'), interval 1 month)
    ),

    source_vitai as (
        select *, concat(id_cnes, "-", data_particao) as id_estabelecimento_particao
        from {{ ref("raw_prontuario_vitai__estoque_posicao") }}
        where data_particao > date_sub(current_date('America/Sao_Paulo'), interval 1 month)
    ),

    source_tpc as (
        select *
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
        where data_particao > date_sub(current_date('America/Sao_Paulo'), interval 1 month)
    ),

    posicao_mais_recente as (
        select * from {{ ref("int_estoque__posicao_mais_recente_por_estabelecimento") }}
    ),

    -- MART
    mart_posicao as (
        select
            sistema_origem,
            sum(material_valor_total) as material_valor_total,
            sum(material_quantidade) as material_quantidade
        from {{ ref("mart_estoque__posicao_atual") }}
        group by 1
    ),

    -- TPC
    tpc as (
        select
            'tpc' as sistema_origem,
            sum(material_valor_total) as material_valor_total,
            sum(material_quantidade) as material_quantidade
        from source_tpc
        where data_particao = (select max(data_particao) from source_tpc)
        group by 1
    ),

    mart_tpc as (
        select
            mart.sistema_origem,
            mart.material_quantidade as mart__material_quantidade,
            origem.material_quantidade as origem__material_quantidade,
            mart.material_valor_total as mart__material_valor_total,
            origem.material_valor_total as origem__material_valor_total,
        from mart_posicao as mart
        left join tpc as origem on mart.sistema_origem = origem.sistema_origem
        where origem.sistema_origem is not null
    ),

    -- VITAI
    vitai as (
        select
            'vitai' as sistema_origem,
            sum(material_valor_total) as material_valor_total,
            sum(material_quantidade) as material_quantidade
        from source_vitai
        left join posicao_mais_recente using (id_estabelecimento_particao)
        where posicao_mais_recente.id_estabelecimento_particao is not null
        group by 1
    ),

    mart_vitai as (
        select
            mart.sistema_origem,
            mart.material_quantidade as mart__material_quantidade,
            origem.material_quantidade as origem__material_quantidade,
            mart.material_valor_total as mart__material_valor_total,
            origem.material_valor_total as origem__material_valor_total,
        from mart_posicao as mart
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
        left join posicao_mais_recente using (id_estabelecimento_particao)
        where posicao_mais_recente.id_estabelecimento_particao is not null
        group by 1
    ),

    mart_vitacare as (
        select
            mart.sistema_origem,
            mart.material_quantidade as mart__material_quantidade,
            origem.material_quantidade as origem__material_quantidade,
            0 as mart__material_valor_total,
            0 as origem__material_valor_total,
        from mart_posicao as mart
        left join vitacare as origem on mart.sistema_origem = origem.sistema_origem
        where origem.sistema_origem is not null
    ),

    -- RESULTADO
    mart_consolidado as (
        select *
        from mart_tpc
        union all
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
