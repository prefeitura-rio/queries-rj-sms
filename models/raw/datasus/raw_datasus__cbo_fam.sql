{{
    config(
        alias="cbo_fam",
    )
}}

with
    source as (select * from {{ source("brutos_datasus_staging", "cbo_fam") }}),
    renamed as (
        select
            safe_cast(chave as string) as id_cbo_familia,
            safe_cast(ds_regra as string) as descricao,
            safe_cast(_data_carga as date format 'DD/MM/YYY') as data_carga,
            safe_cast(_data_snapshot as date format 'DD/MM/YYY') as _data_snapshot,
        from source
    )
select *
from renamed
