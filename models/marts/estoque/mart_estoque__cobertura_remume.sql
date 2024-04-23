{{
    config(
        alias="estoque_cobertura_remume",
        schema="projeto_estoque",
        materialized="table",
    )
}}


-- - Calcula para cada material quantas unidades de saúde de estoque positivo
with
    -- - source
    estoque as (select * from {{ ref("mart_estoque__posicao_atual") }}),

    tpc as (select * from {{ ref("raw_estoque_central_tpc__estoque_posicao") }}),

    -- - transform
    estoque_apv as (
        select
            id_material,
            material_descricao,
            id_cnes,
            sum(material_quantidade) as material_quantidade,
            avg(material_consumo_medio) as material_consumo_medio,
        from estoque
        where
            material_remume_indicador = 'sim'
            and estabelecimento_tipo_sms
            in ('CLINICA DA FAMILIA', 'CENTRO MUNICIPAL DE SAUDE')
        group by id_material, material_descricao, id_cnes
    ),

    estoque_tpc as (
        select id_material, sum(material_quantidade) as material_quantidade_tpc,
        from estoque
        where
            material_remume_indicador = 'sim' and estabelecimento_tipo_sms in ('ESTOQUE CENTRAL')
        group by id_material
    ),

    ultima_data_estoque_tpc as (
        select id_material, max(data_particao) as material_ultima_data_estoque_tpc
        from tpc
        group by id_material
    ),

    -- - target
    final as (
        select
            id_material,
            material_descricao,
            count(distinct id_cnes) as estabelecimentos_contagem,
            count(
                distinct case when material_quantidade > 0 then id_cnes end
            ) as estabelecimentos_estoque_positivo,
            sum(material_quantidade) as material_quantidade,
            sum(material_consumo_medio) as material_consumo_medio,
        from estoque_apv
        group by id_material, material_descricao
        order by estabelecimentos_estoque_positivo, material_descricao
    )

select
    final.*,
    estoque_tpc.material_quantidade_tpc,
    coalesce(
        cast(
            ultima_data_estoque_tpc.material_ultima_data_estoque_tpc
            as string format 'YYYY-MM-DD'),
            '< 2023-10-13' --- periodo quando começamos a coletar os dados da tpc
        ) as material_ultima_data_estoque_tpc,
        if(
            material_quantidade = 0,
            0,
            {{ dbt_utils.safe_divide("material_quantidade", "material_consumo_medio") }}
        ) as estoque_cobertura_dias
        from final
        left join estoque_tpc using (id_material)
        left join ultima_data_estoque_tpc using (id_material)
        order by material_quantidade
