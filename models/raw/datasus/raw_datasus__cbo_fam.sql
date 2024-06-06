{{
    config(
        alias="cbo_fam",
    )
}}

with
    source as (select * from {{ source("brutos_datasus_staging", "cbo_fam") }}),
    renamed as (
        select
            {{ adapter.quote("chave") }},
            {{ adapter.quote("ds_regra") }},
            {{ adapter.quote("_data_carga") }},
            {{ adapter.quote("_data_snapshot") }}
        from source
    )
select *
from renamed
