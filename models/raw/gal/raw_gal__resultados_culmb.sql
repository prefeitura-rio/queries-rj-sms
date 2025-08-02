{{
    config(
        alias="resultados_culmb",
        materialized="table",
        enabled=false
    )
}}

with
    source as (
        select * from {{ source("brutos_gal_staging", "culmb_resultados_2020-2024") }}
    ),
    cleared as (
        select
            {{process_null('id_requisicao')}} as id_requisicao,
            {{process_null('resultado')}} as resultado,
            {{process_null('metodologia')}} as metodologia,
            {{process_null('especie_identificada')}} as especie_identificada,
            {{process_null('metodo_identificacao')}} as metodo_identificacao,
            {{process_null('teste_sensibilidade')}} as teste_sensibilidade,
            {{process_null('tecnica_descontaminacao')}} as tecnica_descontaminacao,
            {{process_null('identificacao_microbacteria')}} as identificacao_microbacteria,
            {{process_null('aspecto_amostra_escarro')}} as aspecto_amostra_escarro,
        from source
    )

select *
from cleared
order by id_requisicao