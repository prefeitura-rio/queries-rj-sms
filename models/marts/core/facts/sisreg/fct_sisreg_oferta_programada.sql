{{
    config(
        schema="saude_sisreg",
        alias="oferta_programada",
        materialized="incremental",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (select * from {{ ref("fct_sisreg_oferta_programada_serie_historica") }})

select *
from source
where data_particao = (select max(data_particao) from source)
