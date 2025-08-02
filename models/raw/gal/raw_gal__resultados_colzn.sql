{{
    config(
        alias="resultados_colzn",
        materialized="table",
        enabled=false
    )
}}

with
    source as (
        select * from {{ source("brutos_gal_staging", "colzn_resultados_2020-2024") }}
    ),
    cleared as (
        select
            {{process_null('id_requisicao')}} as id_requisicao,
            {{process_null('resultado')}} as resultado,
            {{process_null('cultura_prevista_para')}} as cultura_prevista_para,
            {{process_null('aspecto_amostra_escarro')}} as aspecto_amostra_escarro,
        from source
    )

select *
from cleared
order by id_requisicao