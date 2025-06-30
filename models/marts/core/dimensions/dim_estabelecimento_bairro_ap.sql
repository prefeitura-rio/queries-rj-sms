{{
    config(
        schema="saude_dados_mestres",
        alias="estabelecimento_bairro_ap",
        materialized="table",
    )
}}

with
    estabs_bairros_aps as (
        select
            id_cnes,
            
            case
                when id_distrito_sanitario = '' then null
                else id_distrito_sanitario
            end as id_distrito_sanitario,        
            
            -- normalização adhoc
            case
                when endereco_bairro = "COMPLEXO DA MARE"
                then "MARE"
                when endereco_bairro = "CIDADES DE DEUS"
                then "CIDADE DE DEUS"
                when endereco_bairro in ("PORTUGUESA ILHA DO", "PORTUGUESA ILHA DO G")
                then "PORTUGUESA ILHA DO GOV"
                when endereco_bairro = "PRAIA DE RAMOS"
                then "RAMOS"
                else endereco_bairro
            end as bairro

        from {{ ref("raw_cnes_web__estabelecimento") }}
        where
            id_cnes
            in (select id_cnes from {{ ref("raw_sheets__estabelecimento_auxiliar") }})
        qualify row_number() over (partition by id_cnes order by data_carga desc) = 1
    ),

    estabs_bairros_aps_id as (
        select 
            src.id_cnes,
            src.bairro, 
            coalesce(src.id_distrito_sanitario, bairros_aps.ap) as ap

        from estabs_bairros_aps as src
        left join
            {{ ref("raw_sheets__area_programatica_bairros_aps") }} as bairros_aps
            using (bairro)
    ),

    estabs_bairros_aps_titulo as (
        select id.*, titulos.ap_titulo from estabs_bairros_aps_id as id
        left join
            {{ ref("raw_sheets__area_programatica_bairros_aps") }} as titulos
            using (ap)
    )


select *
from estabs_bairros_aps_titulo
