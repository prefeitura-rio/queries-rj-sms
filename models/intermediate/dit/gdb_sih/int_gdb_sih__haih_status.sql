{{
    config(
        schema="intermediario_gdb_sih",
        alias="haih_status",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select
            emissor_gestor,
            numero_aih,

            parse_date("%Y%m", nullif(competencia_processamento, "999999")) as competencia_processamento,
            parse_date("%Y%m", nullif(competencia, "999999")) as competencia,

            id_cnes,
            status,  -- 1, 3, 4?
            emissor_regional,
            parse_date("%Y%m", nullif(prim_competencia, "999999")) as prim_competencia,
            parcela,

            data_particao,
            data_carga

        from {{ ref("raw_gdb_sih__haih_status") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__haih_status") }}
        )
        qualify row_number() over (
            partition by
                -- TODO: o que precisa ser único aqui?
                emissor_gestor,
                numero_aih,
                competencia_processamento,
                competencia,
                id_cnes,
                status,
                emissor_regional,
                prim_competencia,
                parcela
            order by data_carga desc
        ) = 1
    )

select *
from dedup
