{{
    config(
        schema="brutos_sheets",
        alias="projeto_c34_procedimentos_sisreg",
        materialized="table",
        -- TODO: conferir tags
        tag=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}

with
    source as (
        select
            lpad(proced_sisreg_id, 7, '0') as proced_sisreg_id, indicador_cancer_pulmao
        from {{ source("brutos_sheets_staging", "projeto_c34_procedimentos") }}
    )

select *
from source
