{{
    config(
        schema="brutos_sheets",
        alias="usuarios_bigquery",
        tag=["daily"],
    )
}}
-- TODO: conferir tags acima

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "usuarios_bigquery") }}
    )
select
    *
from source
