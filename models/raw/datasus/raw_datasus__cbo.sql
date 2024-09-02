{{
    config(
        alias="cbo",
        schema = "brutos_datasus"
    )
}}

with
    source as (select * from {{ source("brutos_datasus_staging", "cbo") }}),
    renamed as (
        select
            safe_cast(cbo as string) as id_cbo,
            safe_cast(ds_cbo as string) as descricao,
            safe_cast(imported_at as date) as data_carga,
            safe_cast(created_at as date) as _data_snapshot,
        from source
    )
select *
from renamed
