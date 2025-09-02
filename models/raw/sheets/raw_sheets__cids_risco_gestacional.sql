{{
    config(
        schema="brutos_sheets",
        alias="cids_risco_gestacional",
        -- TODO: conferir tags
        tag=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
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
