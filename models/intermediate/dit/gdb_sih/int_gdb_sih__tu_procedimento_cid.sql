{{
    config(
        schema="intermediario_gdb_sih",
        alias="tu_procedimento_cid",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select
            procedimento_codigo,
            cid_codigo,
            case lower(trim(st_principal))
                when "s" then true
                when "n" then false
                else cast(null as bool)
            end as cid_principal,

            parse_date("%Y%m", nullif(competencia_inicio, "999999")) as competencia_inicio,
            parse_date("%Y%m", nullif(competencia_fim, "999999")) as competencia_fim,

            data_particao,
            data_carga

        from {{ ref("raw_gdb_sih__tu_procedimento_cid") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__tu_procedimento_cid") }}
        )
        qualify row_number() over (
            partition by procedimento_codigo
            order by data_carga desc
        ) = 1
    )

select *
from dedup
