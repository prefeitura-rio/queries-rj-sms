{{
    config(
        schema= "brutos_prontuario_vitacare",
        alias="estoque_movimento",
    )
}}

with source as (select * from {{ source('brutos_prontuario_vitacare_staging', 'estoque_movimento') }})

select * from source