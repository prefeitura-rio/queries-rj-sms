{{ config(alias="bairros_aps", schema="brutos_area_programatica") }}

with
    source as (
        select bairro, ap, ap_titulo
        from {{ source("brutos_sheets_staging", "bairros_rio ") }}
    )

select *
from source
