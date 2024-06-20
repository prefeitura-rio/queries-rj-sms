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
            safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
            safe_cast(_data_snapshot as date format 'DD/MM/YYY') as _data_snapshot,
        from source
    )
select *
from renamed
