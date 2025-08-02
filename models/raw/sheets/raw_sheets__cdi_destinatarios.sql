{{ config(schema="brutos_sheets", alias="cdi_destinatarios") }}

with
    source as (
        select * from {{ source("brutos_sheets_staging", "cdi_destinatarios") }}
    )
select *
from source
