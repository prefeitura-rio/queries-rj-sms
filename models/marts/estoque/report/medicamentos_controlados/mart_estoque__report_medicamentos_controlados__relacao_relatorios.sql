--- Contém a relação de relaórios de que devem ser gerados para os estabelecimentos que possuem estoque de medicamentos controlados

{{
    config(
        alias="report_medicamentos_controlados__relacao_relatorios",
        schema="projeto_estoque",
        materialized="table",
        tag=["report", "weekly"],
    )
}}


with
    controlados as (
        select distinct controlado_tipo from {{ ref("dim_material") }} where controlado_indicador = 'sim'
    ),

    cnes as (
        select distinct id_cnes
        from {{ ref("mart_estoque__report_medicamentos_controlados__itens_com_movimento") }}
    ),

    cnes_controlados as (
        select cnes.id_cnes, controlados.controlado_tipo from controlados cross join cnes
    ),

    estabelecimento as (
        select *, concat(endereco_logradouro, ', ', endereco_numero) as endereco
        from {{ ref("dim_estabelecimento") }}
    ),

    farmaceuticos as (
        select id_cnes, array_agg(struct(farmaceutico_nome as nome, farmaceutico_crf as crf)) as farmaceutico
        from {{ ref("raw_sheets__aps_farmacias") }}
        group by 1
    )

select
    cc.id_cnes,
    est.nome_limpo as estabelecimento_nome,
    est.area_programatica as estabelecimento_area_programatica,
    est.endereco as estabelecimento_endereco,
    cc.controlado_tipo,
    f.farmaceutico,
from cnes_controlados as cc
left join estabelecimento as est using (id_cnes)
left join farmaceuticos as f using (id_cnes)
order by est.nome_limpo, cc.controlado_tipo