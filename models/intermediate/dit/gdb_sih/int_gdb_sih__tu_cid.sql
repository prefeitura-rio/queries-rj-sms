{{
    config(
        schema="intermediario_gdb_sih",
        alias="tu_cid",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select
            cid_codigo,
            cid_descricao,
            safe_cast(agravo as int64) as agravo,  -- 0, 1 e 2
            sexo,  -- M, F, I

            parse_date("%Y%m", nullif(competencia_inicio, "999999")) as competencia_inicio,
            parse_date("%Y%m", nullif(competencia_fim, "999999")) as competencia_fim,

            data_particao,
            data_carga

        from {{ ref("raw_gdb_sih__tu_cid") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__tu_cid") }}
        )
        qualify row_number() over (
            partition by cid_codigo
            order by data_carga desc
        ) = 1
    )

select *
from dedup
