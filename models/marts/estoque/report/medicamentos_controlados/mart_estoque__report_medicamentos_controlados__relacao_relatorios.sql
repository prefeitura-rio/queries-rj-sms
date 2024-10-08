-- - Contém a relação de relaórios de que devem ser gerados para os estabelecimentos
-- que possuem estoque de medicamentos controlados
{{
    config(
        enabled= false,
        alias="report_medicamentos_controlados__relacao_relatorios",
        schema="projeto_estoque",
        materialized="table",
        tag=["report", "weekly"],
    )
}}


with
    controlados as (
        select distinct controlado_tipo
        from {{ ref("dim_material") }}
        where controlado_indicador = 'sim'
    ),

    cnes as (
        select *, concat(endereco_logradouro, ', ', endereco_numero) as endereco
        from {{ ref("dim_estabelecimento") }}
        where prontuario_versao = 'vitacare' and prontuario_estoque_tem_dado = 'sim'
    ),

    cnes_controlados as (
        select
            cnes.id_cnes,
            cnes.nome_limpo,
            cnes.area_programatica,
            cnes.endereco,
            controlados.controlado_tipo
        from controlados
        cross join cnes
    ),

    farmaceuticos as (
        select
            id_cnes,
            array_agg(
                struct(farmaceutico_nome as nome, farmaceutico_crf as crf)
            ) as farmaceutico
        from {{ ref("raw_sheets__aps_farmacias") }}
        group by 1
    ),

    final as (
        select
            cc.id_cnes,
            cc.nome_limpo as estabelecimento_nome,
            cc.area_programatica as estabelecimento_area_programatica,
            cc.endereco as endereco_farmacia,
            cc.controlado_tipo,
            f.farmaceutico,
        from cnes_controlados as cc
        left join farmaceuticos as f using (id_cnes)
        order by cc.nome_limpo, cc.controlado_tipo
    )

select *
from final
