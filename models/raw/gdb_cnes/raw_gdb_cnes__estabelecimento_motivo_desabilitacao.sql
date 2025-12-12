{{
    config(
        alias="estabelecimento_motivo_desabilitacao",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source("brutos_gdb_cnes_staging", "NFCES049") }}
),
renamed as (
    select
        cast(CD_MOTIVO_DESAB as string) as id_motivo_desabilitacao,
        cast(DS_MOTIVO_DESAB as string) as motivo_desativacao,
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

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from source
)

select *
from renamed
