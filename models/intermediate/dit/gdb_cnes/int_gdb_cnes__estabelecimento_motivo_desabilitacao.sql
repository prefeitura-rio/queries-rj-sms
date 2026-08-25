{{
    config(
        schema="intermediario_gdb_cnes",
        alias="estabelecimento_motivo_desabilitacao",
        materialized="table",
        tags=["gdb_cnes"]
    )
}}


with
    base as (
        select
            cast({{ process_null("CD_MOTIVO_DESAB") }} as string) as id_motivo_desabilitacao,
            cast({{ process_null("DS_MOTIVO_DESAB") }} as string) as motivo_desativacao,
            case
                when trim(TP_MOTIVO_DESAB)='1' then 'manual'
                when trim(TP_MOTIVO_DESAB)='2' then 'automática'
                else null
            end as tipo_desativacao,
            case
                when trim(lower(FL_DEFINITIVO))='s' then true
                when trim(lower(FL_DEFINITIVO))='n' then false
                else null
            end as desativacao_definitiva,

            data_particao as competencia,
            _loaded_at

        from {{ ref("raw_gdb_cnes__nfces049") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_cnes__nfces049") }}
        )
        qualify row_number() over (
            partition by id_motivo_desabilitacao
            order by _loaded_at desc
        ) = 1
    )

select *
from base
