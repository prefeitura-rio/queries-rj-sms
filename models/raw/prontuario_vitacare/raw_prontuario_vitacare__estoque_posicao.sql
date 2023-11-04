{{
    config(
        schema= "brutos_prontuario_vitacare",
        alias="estoque_posicao",
    )
}}

with source as (select * from {{ source('brutos_prontuario_vitacare_staging', 'estoque_posicao') }})

select * from source