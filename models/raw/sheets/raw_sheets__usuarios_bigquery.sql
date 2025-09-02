{{
    config(
        schema="brutos_sheets",
        alias="usuarios_bigquery",
        -- TODO: conferir tags
        tag=["daily"],
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "usuarios_bigquery") }}
    )
select
    *
from source
