{{
    config(
        alias="cbo",
    )
}}

with
    source as (select * from {{ source("brutos_datasus_staging", "cbo") }}),
    renamed as (
        select
            {{ adapter.quote("cbo") }},
            {{ adapter.quote("ds_cbo") }},
            {{ adapter.quote("_data_carga") }},
            {{ adapter.quote("_data_snapshot") }}
        from source
    )
select *
from renamed
