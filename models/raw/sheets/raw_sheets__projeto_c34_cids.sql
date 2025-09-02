{{
    config(
        schema="brutos_sheets",
        alias="projeto_c34_cids",
        materialized="table",
        tag=["daily", "subgeral", "cnes_subgeral", "monitora_reg"],
    )
}}
-- TODO: conferir tags acima

with
    source as (
        select
            cid,
            indicador_cancer,
            indicador_cancer_pulmao,
            row_number() over (partition by cid order by cid) as rn
        from {{ source("brutos_sheets_staging", "projeto_c34_cids") }}
    )

select cid, indicador_cancer, indicador_cancer_pulmao
from source
where rn = 1
