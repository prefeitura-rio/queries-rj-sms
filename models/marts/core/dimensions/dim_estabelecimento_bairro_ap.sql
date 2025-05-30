{{
    config(
        schema="saude_dados_mestres",
        alias="estabelecimento_bairro_ap",
        materialized="table",
    )
}}

with
    estabs_bairros as (
        select
            id_cnes,

            -- temp
            case
                when endereco_bairro = "COMPLEXO DA MARE"
                then "MARE"
                when endereco_bairro = "COCOTA ILHA DO GOV"
                then "COCOTA"
                when endereco_bairro = "PORTUGUESA"
                then "PORTUGUESA ILHA DO G"
                else endereco_bairro
            end as endereco_bairro

        from {{ ref("raw_cnes_web__estabelecimento") }}
        qualify row_number() over (partition by id_cnes order by data_carga desc) = 1
    ),

    enriquece_aps as (
        select
            id_cnes,
            endereco_bairro as bairro,
            area_programatica,
            area_programatica_descr
        from {{ ref("raw_sheets__estabelecimento_auxiliar") }}
        left join estabs_bairros using (id_cnes)
    ),

    estabs_bairros_aps as (
        select
            id_cnes,
            bairro,
            coalesce(cast(area_programatica as int), ap) as ap,
            coalesce(area_programatica_descr, ap_titulo) as ap_titulo

        from enriquece_aps
        left join {{ ref("raw_area_programatica__bairros_aps") }} using (bairro)
    )

select *
from estabs_bairros_aps
