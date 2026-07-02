{{
    config(
        schema="brutos_sheets",
        alias="unidades_exames_laboratoriais",
        tags=["monthly", "exames_laboratoriais"],
        meta={"owner": "daniel"}
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "unidades_exames_laboratoriais") }}
    )
select *
from source
