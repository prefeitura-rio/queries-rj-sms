{{
    config(
        alias="resultados_pcr",
        materialized="table",
        enabled=false
    )
}}

with
    source as (
        select * from {{ source("brutos_gal_staging", "pcr_resultados_2020-2024") }}
    ),
    cleared as (
        select
            {{process_null('id_requisicao')}} as id_requisicao,
            {{process_null('dna_complexo_mt')}} as dna_complexo_mt,
            {{process_null('rifampicina')}} as rifampicina,
            {{process_null('complemento')}} as complemento,
            {{process_null('aspecto_amostra_escarro')}} as aspecto_amostra_escarro,
        from source
    )

select *
from cleared
order by id_requisicao