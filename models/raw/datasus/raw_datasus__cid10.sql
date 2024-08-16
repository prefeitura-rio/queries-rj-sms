{{
    config(
        alias="cid10",
        schema = "brutos_datasus"
    )
}}

with
    source as (select * from {{ source("brutos_datasus_staging", "cid10") }}),
    renamed as (
        select
            safe_cast(SUBCAT as string) as codigo_cid,
            safe_cast(DESCRICAO as string) as descricao
        from source
    )
select *
from renamed
