{{
    config(
        schema="brutos_sheets",
        alias="projeto_c34_procedimentos_ser",
        materialized="table",
    )
}}

with
    source as (
        select
            procedimento_ser as procedimento, indicador_cancer_pulmao, indicador_cancer
        from {{ source("brutos_sheets_staging", "procedimentos_ser") }}
    )

select *
from source
