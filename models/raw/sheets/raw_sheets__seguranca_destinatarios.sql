{{
    config(
        schema="brutos_sheets",
        alias="seguranca_destinatarios",
        tags=["informes-seguranca", "daily"],
        meta={"owner": "avellar"}
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "seguranca_destinatarios") }}
    )
select *
from source
