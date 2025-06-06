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
            trim(upper(string_field_0)) as procedimento,
            trim(upper(string_field_1)) as indicador_cancer_pulmao,
            trim(upper(string_field_2)) as indicador_cancer
        from {{ source("brutos_sheets_staging", "c34_procedimentos_ser") }}
    )

select *
from source
