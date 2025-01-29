{{ config(
    schema="brutos_monitoramento",
    alias="healthchecks",
    tags=["monitoramento"]
) }}


with
    source as (
        select *
        from {{ source("brutos_monitoramento_staging", "healthchecks") }}
    )
select *
from source
