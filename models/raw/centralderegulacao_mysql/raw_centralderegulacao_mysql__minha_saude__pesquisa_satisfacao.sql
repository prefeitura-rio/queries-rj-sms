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
                    "dw__vw_minhasauderio_pesquisa_satisfacao",
                )
            }}
    ),
    deduplicated as (
        select *
        from source
        qualify row_number() over (partition by cpf order by datahoraresposta desc) = 1
    )
select *
from deduplicated
