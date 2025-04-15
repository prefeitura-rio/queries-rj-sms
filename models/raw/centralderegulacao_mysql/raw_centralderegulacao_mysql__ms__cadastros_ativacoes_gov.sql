{{
    config(
        schema="brutos_centralderegulacao_mysql", alias="ms__cadastros_ativacoes_gov"
    )
}}

with
    source as (
        select *
        from
            {{
                source(
                    "brutos_centralderegulacao_mysql_staging",
                    "monitoramento__vw_MS_CadastrosAtivacoesGov",
                )
            }}
    ),

    deduplicated as (
        select *
        from source
        qualify row_number() over (partition by dia order by datalake_loaded_at desc) = 1
    )
select *
from deduplicated
