{{
    config(
        schema="brutos_centralderegulacao_mysql", alias="ms__cadastros_ativacoes_gov"
    )
}}

with
    source as (
        select
            safe_cast(dia as date) as dia,
            safe_cast(split(cadastroativo, '.')[0] as int64) as cadastroativo,
            safe_cast(split(cadastronaoativo, '.')[0] as int64) as cadastronaoativo,
            safe_cast(qtdtotal as int64) as qtdtotal,
            safe_cast(split(cadastrosgovbr, '.')[0] as int64) as cadastrosgovbr,
            safe_cast(porcativogovbr as float64) as porcativogovbr,
            safe_cast(porcinativo as float64) as porcinativo,
            safe_cast(data_extracao as date) as data_extracao,
            safe_cast(ano_particao as int64) as ano_particao,
            safe_cast(mes_particao as int64) as mes_particao,
            safe_cast(data_particao as date) as data_particao

        from
            {{
                source(
                    "brutos_centralderegulacao_mysql_staging",
                    "vw_MS_CadastrosAtivacoesGov",
                )
            }}
    ),

    deduplicated as (
        select *
        from source
        qualify
            row_number() over (partition by dia order by data_extracao desc) = 1
    )
select *
from deduplicated
