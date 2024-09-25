-- verifica se os valores consolidados no mart de posicao atual batem com os valores
-- encontrados que estão chegando da tpc, vitai e vitacare
with

    -- SOURCES
    -- filtra dados de 1 mês a fim de evitar full scan das tabelas.
    -- Premissa: Nenhuma ingestão deve estar 1 mês atrasada
    estabelecimento as (select * from {{ ref("dim_estabelecimento") }}),

    raw_posicao_mais_recente as (
        select * from {{ ref("int_estoque__posicao_mais_recente_por_estabelecimento") }}
    ),

    raw_vitacare as (
        select
            id_cnes,
            data_particao as raw__data_particao,
            "vitacare" as sistema_origem,
            sum(0) as raw__material_valor_total,
            sum(material_quantidade) as raw__material_quantidade
        from {{ ref("raw_prontuario_vitacare__estoque_posicao") }}
        inner join raw_posicao_mais_recente using (id_cnes, data_particao)
        group by 1, 2, 3
    ),

    raw_vitai as (
        select
            id_cnes,
            data_particao as raw__data_particao,
            "vitai" as sistema_origem,
            sum(material_valor_total) as raw__material_valor_total,
            sum(material_quantidade) as raw__material_quantidade
        from {{ ref("raw_prontuario_vitai__estoque_posicao") }}
        inner join raw_posicao_mais_recente using (id_cnes, data_particao)
        group by 1, 2, 3
    ),

    raw_tpc as (
        select
            'tpc' as id_cnes,
            data_particao as raw__data_particao,
            "tpc" as sistema_origem,
            sum(material_valor_total) as raw__material_valor_total,
            sum(material_quantidade) as raw__material_quantidade
        from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}
        where
            data_particao
            = (select data_particao from raw_posicao_mais_recente where id_cnes = 'tpc')
        group by 1, 2, 3
    ),

    raw_consolidado as (
        select *
        from raw_vitai
        union all
        select *
        from raw_vitacare
        union all
        select *
        from raw_tpc
    ),

    -- -- MART
    mart_posicao_atual_consolidado as (
        select
            id_cnes,
            data_particao as mart__data_particao,
            sistema_origem,
            sum(material_valor_total) as mart__material_valor_total,
            sum(material_quantidade) as mart__material_quantidade
        from {{ ref("mart_estoque__posicao_atual") }}
        group by 1, 2, 3
    ),

    final as (
        select
            mart.id_cnes,
            est.nome_limpo as nome,
            est.area_programatica as area_programatica,
            mart.sistema_origem,
            raw.raw__data_particao,
            mart.mart__data_particao,
            raw.raw__material_quantidade,
            mart.mart__material_quantidade,
            raw.raw__material_valor_total,
            mart.mart__material_valor_total
        from mart_posicao_atual_consolidado as mart
        left join raw_consolidado as raw using (id_cnes)
        left join estabelecimento as est using (id_cnes)
    )

select *
from final
where
    mart__data_particao <> raw__data_particao
    or abs(mart__material_quantidade - raw__material_quantidade) > 0.1
    or (
        abs(mart__material_valor_total - raw__material_valor_total) > 0.1
        and sistema_origem <> 'vitacare' -- vitacare não tem valor de estoque na camada raw
    )
    -- adicionada uma tolerancia de .1 para evitar falsos positivos devido ao
    -- truncamento de casas decimais
    
    
