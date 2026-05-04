{{
    config(
        schema="brutos_sheets",
        alias="codigos_exames",
        tags=["monthly"],
        meta={"owner": "avellar"}
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "codigos_exames") }}
    )
select *
from source
