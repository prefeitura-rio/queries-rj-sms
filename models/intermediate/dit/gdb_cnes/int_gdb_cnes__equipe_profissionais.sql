{{
    config(
        schema="intermediario_gdb_cnes",
        alias="equipe_profissionais",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}


with
    profissional as (
        select *
        from {{ ref("raw_gdb_cnes__equipe_profissionais") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__equipe_profissionais") }}
        )
        qualify row_number() over (
            partition by id_profissional_sus, id_unidade, id_cbo, equipe_sequencial
            order by data_carga desc
        ) = 1
    )

select *
from profissional
