{{
    config(
        schema="intermediario_gdb_sih",
        alias="faixa_aih",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select
            emissor_gestor,
            faixa_inicio,
            faixa_fim,

            parse_date("%Y%m", nullif(competencia_inicio, "999999")) as competencia_inicio,
            parse_date("%Y%m", nullif(competencia_fim, "999999")) as competencia_fim,

            data_particao,
            data_carga

        from {{ ref("raw_gdb_sih__faixa_aih") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__faixa_aih") }}
        )
        qualify row_number() over (
            partition by emissor_gestor, faixa_inicio, faixa_fim
            order by data_carga desc
        ) = 1
    )

select *
from dedup
