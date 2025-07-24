{{
    config(
        alias="tipo_resultado",
        materialized="table",
    )
}}

with
    source as (
        select * from {{ source("brutos_gal_staging", "tipo_resultado") }}
    ),

    cleaned as (
        select
            {{process_null('id')}} as id,
            {{process_null('descricao')}} as descricao,
        from source
    ),

    final as (
        select 
            *
        from cleaned
    )


select *
from final
order by id