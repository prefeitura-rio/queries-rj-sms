{{
    config(
        schema="brutos_sheets",
        alias="cids_risco_gestacional",
    )
}}

with
    source as (
        select
            *
        from {{ source("brutos_sheets_staging", "cids_risco_gestacional") }}
    )

select *
from source