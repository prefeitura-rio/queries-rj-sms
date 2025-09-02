{{
    config(
        enabled= false,
        alias="report_medicamentos_controlados__itens_sem_movimento",
        schema="projeto_estoque",
        materialized="table",
        tags=["report", "weekly"],
    )
}}


with
    controlados as (
        select * from {{ ref("dim_material") }} where controlado_indicador = 'sim'
    ),

    cnes as (
        select distinct id_cnes
        from {{ ref("mart_estoque__report_medicamentos_controlados__itens_com_movimento") }}
    ),

    cnes_controlados as (
        select cnes.id_cnes, controlados.id_material from controlados cross join cnes
    ),

    cnes_controlados_com_movimentacao as (
        select distinct id_cnes, id_material
        from {{ ref("mart_estoque__report_medicamentos_controlados__itens_com_movimento") }}
    ),

    cnes_controlados_sem_movimentacao as (
        select cc.id_cnes, cc.id_material
        from cnes_controlados as cc
        left join
            cnes_controlados_com_movimentacao as cc_com_mov
            on cc.id_cnes = cc_com_mov.id_cnes
            and cc.id_material = cc_com_mov.id_material
        where cc_com_mov.id_cnes is null
    ),

    posicao_atual as (
        select *
        from {{ ref("mart_estoque__posicao_atual_agregado") }}
        inner join cnes_controlados_sem_movimentacao using (id_material, id_cnes)
    ),

    estabelecimento as (
        select *, concat(endereco_logradouro, ', ', endereco_numero) as endereco
        from {{ ref("dim_estabelecimento") }}
    )

select
    sem_mov.id_cnes,
    est.nome_limpo as estabelecimento_nome,
    est.area_programatica as estabelecimento_area_programatica,
    est.endereco_numero as estabelecimento_endereco,
    sem_mov.id_material,
    upper(
        trim(
            regexp_replace(
                regexp_replace(normalize(mat.nome, nfd), r"\pM", ''),
                r'[^ A-Za-z0-9.,]',
                ' '
            )
        )
    ) as material_nome,
    mat.controlado_tipo,
    coalesce(pos.material_quantidade, 0) as posicao_atual
from cnes_controlados_sem_movimentacao as sem_mov
left join posicao_atual as pos using (id_material, id_cnes)
left join controlados as mat using (id_material)
left join estabelecimento as est using (id_cnes)
