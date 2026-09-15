{{
    config(
        schema="intermediario_gdb_sih",
        alias="tu_procedimento",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select
            procedimento_codigo,
            procedimento_nome,
            complexidade,
            sexo,
            cast(
                nullif(maximo_execucoes, "9999")
                as int64
            ) as maximo_execucoes,
            cast(
                nullif(permanencia_dias, "9999")
                as int64
            ) as permanencia_dias,
            cast(pontos_quantidade as int64) as pontos_quantidade,

            cast(
                nullif(idade_minima, "9999")
                as int64
            ) as idade_minima,
            cast(
                nullif(idade_maxima, "9999")
                as int64
            ) as idade_maxima,

            cast(valor_sh as float64) as valor_sh,
            cast(valor_sa as float64) as valor_sa,
            cast(valor_sp as float64) as valor_sp,

            parse_date("%Y%m", nullif(vigencia_inicio, "999999")) as vigencia_inicio,
            parse_date("%Y%m", nullif(vigencia_fim, "999999")) as vigencia_fim,

            financiamento_codigo,
            rubrica_codigo,

            data_particao,
            data_carga

        from {{ ref("raw_gdb_sih__tu_procedimento") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__tu_procedimento") }}
        )
        qualify row_number() over (
            partition by procedimento_codigo
            order by data_carga desc
        ) = 1
    )

select *
from dedup
