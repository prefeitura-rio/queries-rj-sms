{{
    config(
        schema="brutos_centralderegulacao_mysql",
        alias="tea_relatorio",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with
    source as (
        select *
        from {{ source("brutos_centralderegulacao_mysql_staging", "vw_tea_relatorio") }}
    ),
    final as (select * from source)
select *
from final
