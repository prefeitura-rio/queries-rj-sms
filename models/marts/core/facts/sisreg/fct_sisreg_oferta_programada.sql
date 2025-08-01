{{
    config(
        schema="saude_sisreg",
        alias="oferta_programada",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

{% set last_partition = get_last_partition_date( this ) %}

select *
from {{ ref("fct_sisreg_oferta_programada_serie_historica") }}
where data_particao >= '{{ last_partition }}'