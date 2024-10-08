{{
    config(
        alias="cid10",
        schema="brutos_datasus",
        labels={
            "dado_publico": "sim",
            "dado_pessoal": "nao",
            "dado_anonimizado": "nao",
            "dado_sensivel_saude": "nao",
        },
    )
}}

with
    source as (select * from {{ source("brutos_datasus_staging", "cid10") }})

select *
from source
order by ordem
