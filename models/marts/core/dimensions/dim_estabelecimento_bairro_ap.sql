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

            case
                when endereco_bairro = "COMPLEXO DA MARE"
                then "MARE"
                when endereco_bairro = "COCOTA ILHA DO GOV"
                then "COCOTA"
                else endereco_bairro
            end as bairro

        from {{ ref("raw_cnes_web__estabelecimento") }}
        qualify row_number() over (partition by id_cnes order by data_carga desc) = 1
    ),

    estabs_bairros_aps as (
        select src.id_cnes, src.bairro, bairros_aps.ap as ap, bairros_aps.ap_titulo

        from estabs_bairros as src
        left join
            {{ ref("raw_sheets__area_programatica_bairros_aps") }} as bairros_aps
            using (bairro)
    )

select *
from estabs_bairros_aps
