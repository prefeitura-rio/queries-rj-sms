-- Este teste verifica se as evolução na posicao de estoque do último dia é 
-- explicada pelas entradas e saídas observadas na tabela movimento do dia anterior
{{ config(
    severity = "error",
    error_if = ">2000",
    warn_if = ">1000",
    store_failures = true,
    
) }}
with
    -- sources
    posicao as (
        select
            id_cnes,
            id_material,
            data_particao,
            sum(material_quantidade) as material_quantidade
        from {{ ref("fct_estoque_posicao") }}
        where
            data_particao >= date_sub(current_date('America/Sao_Paulo'), interval 1 day)
            and sistema_origem <> "tpc"  -- não temos movimentos de estoque da TPC
        group by id_cnes, id_material, data_particao
    ),
    movimento as (
        select
            id_cnes,
            id_material,
            sum(material_quantidade_com_sinal) as material_quantidade_delta
        from {{ ref("fct_estoque_movimento") }}
        where data_particao = date_sub(current_date('America/Sao_Paulo'), interval 1 day)
        group by id_cnes, id_material
    ),
    estalelecimento as (select * from {{ ref("dim_estabelecimento") }}),

    -- junta a posicao do dia atual com a do dia anterior e 
    posicao_atual as (select * from posicao where data_particao = current_date('America/Sao_Paulo')),
    posicao_anterior as (
        select *
        from posicao
        where data_particao = date_sub(current_date('America/Sao_Paulo'), interval 1 day)
    ),
    posicao_final as (
        select
            atual.id_cnes,
            atual.id_material,
            anterior.material_quantidade as material_quantidade_anterior,
            atual.material_quantidade as material_quantidade_atual,
        from posicao_atual atual
        left join
            posicao_anterior anterior
            on atual.id_cnes = anterior.id_cnes
            and atual.id_material = anterior.id_material
        where anterior.material_quantidade is not null  -- precisa ter posicao anterior
    ),

    -- resultados consolidados
    consolidado as (
        select
            est.prontuario_versao,
            est.area_programatica,
            est.nome_limpo,
            pos.*,
            mov.material_quantidade_delta as delta_movimento,
            pos.material_quantidade_atual
            - pos.material_quantidade_anterior as delta_posicao,
            pos.material_quantidade_atual
            - pos.material_quantidade_anterior
            - mov.material_quantidade_delta as delta_diferenca,
            abs(
                pos.material_quantidade_atual
                - pos.material_quantidade_anterior
                - mov.material_quantidade_delta
            ) as delta_diferenca_absoluta,
            abs(
                {{ dbt_utils.safe_divide("pos.material_quantidade_atual
                - pos.material_quantidade_anterior
                - mov.material_quantidade_delta", "pos.material_quantidade_atual") }}
                  
            ) as delta_diferenca_percentual,
        from posicao_final as pos
        left join
            movimento as mov
            on pos.id_cnes = mov.id_cnes
            and pos.id_material = mov.id_material
        left join estalelecimento as est on pos.id_cnes = est.id_cnes
        where mov.id_cnes is not null
    )  -- precisa ter movimento

select * from consolidado
where delta_diferenca_percentual > 0.10 -- tolerancia por descasamento de dados
order by delta_diferenca_percentual desc
