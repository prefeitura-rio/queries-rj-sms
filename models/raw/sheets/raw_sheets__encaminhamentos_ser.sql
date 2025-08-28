{{
    config(
        schema="brutos_sheets",
        alias="encaminhamentos_ser",
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
