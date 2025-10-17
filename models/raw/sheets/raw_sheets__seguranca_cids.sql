{{
    config(
        schema="brutos_sheets",
        alias="seguranca_cids",
        tags=["informes-seguranca", "daily"],
    )
}}

with
    source as (
        select
            cid,
            -- descricao
        from {{ source("brutos_sheets_staging", "seguranca_cids") }}
    )
select *
from source
