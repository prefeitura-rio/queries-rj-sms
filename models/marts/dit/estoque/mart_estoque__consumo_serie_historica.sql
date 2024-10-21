{{
    config(
        alias="estoque_consumo_serie_historica",
        schema="projeto_estoque",
        materialized="table",
    )
}}

with

dispensacao as (
    select 
        id_cnes, 
        id_material, 
        data, 
        dia_semana, 
        quantidade_estocada,
        quantidade_dispensada,
        row_num,
        q1,
        q3,
        iqr,
        outlier
    from {{ ref('int_estoque__dispensacao_serie_historica_calculo_cmd') }}),

estabelecimento as (select * from {{ ref('dim_estabelecimento') }}),

material_dimensao as (select * from {{ ref('dim_material') }}),

material_alternativo as (select distinct id_cnes, id_material, material_descricao from {{ ref('mart_estoque__posicao_atual') }})

select 
    d.*,
    if(d.outlier = "sim", null, d.quantidade_dispensada) as quantidade_dispensada_sem_outlier,
    if(d.outlier = "sim", d.quantidade_dispensada, null) as quantidade_dispensada_somente_outlier,
    e.area_programatica as estabelecimento_area_programatica,
    e.nome_limpo as estabelecimento_nome_limpo,
    e.nome_sigla as estaabelecimento_nome_sigla,
    coalesce(md.nome,ma.material_descricao) as material_descricao,
    concat(d.id_material, "- ",coalesce(md.nome, ma.material_descricao)) as material_id_descricao
from dispensacao as d
left join estabelecimento as e on d.id_cnes = e.id_cnes
left join material_dimensao as md on d.id_material = md.id_material
left join material_alternativo as ma on d.id_material = ma.id_material and d.id_cnes = ma.id_cnes
order by d.id_cnes, d.id_material, d.row_num
