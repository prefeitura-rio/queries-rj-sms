{{
    config(
        alias="agravos",
        materialized="table",
    )
}}

with
    source as (
        select * from {{ source("brutos_gal_staging", "agravos") }}
    ),

    cleaned as (
        select
            {{process_null('id')}} as id,
            {{process_null('nome')}} as nome,
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