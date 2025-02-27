{{
    config(
        schema="brutos_centralderegulacao_mysql",
        alias="fibromialgia_relatorio",
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
        from
            {{
                source(
                    "brutos_centralderegulacao_mysql_staging",
                    "vw_fibromialgia_relatorio",
                )
            }}
    ),
    final as (select * from source)
select *
from final
