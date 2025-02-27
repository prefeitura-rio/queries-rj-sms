{{
    config(
        schema="brutos_centralderegulacao_mysql",
        alias="minha_saude__pesquisa_satisfacao",
    )
}}

with
    source as (
        select *
        from
            {{
                source(
                    "brutos_centralderegulacao_mysql_staging",
                    "vw_minhasauderio_pesquisa_satisfacao",
                )
            }}
    ),
    final as (select * from source)
select *
from final
