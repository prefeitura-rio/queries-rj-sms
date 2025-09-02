{{
    config(
        schema="brutos_sheets",
        alias="encaminhamentos_ser",
        tags=["daily"],
    )
}}

with
    source as (
        select
            *
        from {{ source("brutos_sheets_staging", "encaminhamentos_ser") }}
    )

select *
from source
