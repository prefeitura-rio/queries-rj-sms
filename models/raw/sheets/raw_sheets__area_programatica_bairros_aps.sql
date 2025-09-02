{{
    config(
        alias="bairros_aps",
        schema="brutos_sheets",
        -- TODO: conferir tags
        tag=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}

with
    source as (
        select bairro, ap, ap_titulo
        from {{ source("brutos_sheets_staging", "bairros_rio") }}
    )

select *
from source
