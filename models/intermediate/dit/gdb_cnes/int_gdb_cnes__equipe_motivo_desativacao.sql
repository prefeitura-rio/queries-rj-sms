{{
    config(
        schema="intermediario_gdb_cnes",
        alias="equipe_motivo_desativacao",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}


with
    base as (
        select
          cast({{ process_null("CD_MOTIVO_DESATIV") }} as string) as id_motivo_desativacao,
          cast({{ process_null("DS_MOTIVO_DESATIV") }} as string) as motivo_desativacao,

          data_particao as competencia,
          _loaded_at

        from {{ ref("raw_gdb_cnes__nfces053") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces053") }}
        )
        qualify row_number() over (
            partition by id_motivo_desativacao
            order by _loaded_at desc
        ) = 1
    )

select *
from base
