{{
    config(
        schema="brutos_centralderegulacao_mysql",
        alias="ms__cadastros_ativacoes_gov",
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
                    "vw_MS_CadastrosAtivacoesGov",
                )
            }}
    ),

    final as (select * from source)
select *
from final
