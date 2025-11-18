{{
    config(
        alias="equipe_motivo_desativacao",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source("brutos_gdb_cnes_staging", "NFCES053") }}
),
renamed as (
    select
        cast(CD_MOTIVO_DESATIV as string) as id_motivo_desativacao,
        cast(DS_MOTIVO_DESATIV as string) as motivo_desativacao,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from source
)

select *
from renamed
