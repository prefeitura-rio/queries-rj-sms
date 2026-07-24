{{
    config(
        schema="brutos_sheets",
        alias="depara_vacinas",
        tags=["monthly", "vacinacao"],
        meta={"owner": "daniel"}
    )
}}

-- Depara criado para padronizar nomes das vacinas de diversas fontes

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "depara_vacinas") }}
    )
select *
from source