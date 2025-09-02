{{
    config(
        schema="brutos_sheets",
        alias="projeto_c34_procedimentos_sisreg",
        materialized="table",
        tags=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}
-- TODO: conferir tags acima

with
    source as (
        select
            lpad(proced_sisreg_id, 7, '0') as proced_sisreg_id, indicador_cancer_pulmao
        from {{ source("brutos_sheets_staging", "projeto_c34_procedimentos") }}
    )

select *
from source
