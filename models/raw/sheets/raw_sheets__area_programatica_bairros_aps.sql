{{
    config(
        alias="bairros_aps",
        schema="brutos_sheets",
        tags=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}
-- TODO: conferir tags acima

with
    source as (
        select bairro, ap, ap_titulo
        from {{ source("brutos_sheets_staging", "bairros_rio") }}
    )

select *
from source
