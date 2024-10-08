{{
    config(
        alias="cbo_fam",
        schema = "brutos_datasus"
    )
}}

with
    source as (select * from {{ source("brutos_datasus_staging", "cbo_fam") }}),
    renamed as (
        select
            safe_cast(chave as string) as id_cbo_familia,
            safe_cast(ds_regra as string) as descricao,
            safe_cast(imported_at as date) as data_carga,
            safe_cast(created_at as date) as _data_snapshot,
        from source
    )
select *
from renamed