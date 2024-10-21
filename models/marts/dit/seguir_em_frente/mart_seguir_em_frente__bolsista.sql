{{
    config(
        alias="bolsista",
        materialized="table",
        unique_key="id",
    )
}}


with source as (select * from {{ ref("raw_seguir_em_frente__bolsista") }})

select * except (ano_particao, mes_particao, data_particao)
from source
where data_particao = (select max(data_particao) as max_data_particao from source)
