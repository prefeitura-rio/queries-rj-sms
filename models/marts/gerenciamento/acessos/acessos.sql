{{
    config(
        schema="gerenciamento_acessos",
        alias="looker_farmacia_digital",
    )
}}

with
    source as (
        select * from {{ ref("raw_sheets__gerenciamento_acesso_looker_farmacia") }}
    )

select *
from source
