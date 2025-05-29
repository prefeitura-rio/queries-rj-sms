{{ config(schema="brutos_sheets", alias="projeto_c34_cids", materialized="table") }}

with
    source as (
        select cid, indicador_cancer, indicador_cancer_pulmao
        from {{ source("brutos_sheets_staging", "projeto_c34_cids") }}
    )

select *
from source
