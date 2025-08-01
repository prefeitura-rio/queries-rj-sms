{{
    config(
        alias="resultados_tsa",
        materialized="table",
        enabled=false
    )
}}

with
    source as (
        select * from {{ source("brutos_gal_staging", "tsa_resultados_2020-2024") }}
    ),

    cleaned as (
        select	
            {{process_null('id_requisicao')}} as id_requisicao,
            {{process_null('agente_etiologico')}} as agente_etiologico,
            {{process_null('tecnica')}} as tecnica,

            {{process_null('estreptomicina')}} as estreptomicina,
            {{process_null('isoniazida')}} as isoniazida,
            {{process_null('rifampicina')}} as rifampicina,
            {{process_null('etambutol')}} as etambutol,
            {{process_null('kanamicina')}} as kanamicina,
            {{process_null('pirazinamida')}} as pirazinamida,
            {{process_null('etionamida')}} as etionamida,
            {{process_null('amicacina')}} as amicacina,
            {{process_null('ofloxacina')}} as ofloxacina,
            {{process_null('capreomicina')}} as capreomicina,
            {{process_null('levofloxacina')}} as levofloxacina,
            {{process_null('moxifloxacina')}} as moxifloxacina,
            {{process_null('protionamida')}} as protionamida,

            {{process_null('antibiotico_1')}} as antibiotico_1,
            {{process_null('result_outro_antibiotico_1')}} as antibiotico_1_outro_resultado,
            {{process_null('antibiotico_2')}} as antibiotico_2,
            {{process_null('result_outro_antibiotico_2')}} as antibiotico_2_outro_resultado,
            {{process_null('antibiotico_3')}} as antibiotico_3,
            {{process_null('result_outro_antibiotico_3')}} as antibiotico_3_outro_resultado,
        from source
    ),

    final as (
        select *
        from cleaned
    )


select *
from final
order by id_requisicao