{{
    config(
        schema="brutos_sheets",
        alias="medicamentos_uso_continuo",
        materialized="table",
    )
}}

select * from {{source("brutos_sheets_staging", "medicamentos_uso_continuo")}}