{{
    config(
        alias="equipe_motivo_desativacao",
        schema= "brutos_gdb_cnes",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source("brutos_gdb_cnes_staging", "NFCES053") }}
),
extracted as (
    select
        json_extract_scalar(json, "$.CD_MOTIVO_DESATIV") as CD_MOTIVO_DESATIV,
        json_extract_scalar(json, "$.DS_MOTIVO_DESATIV") as DS_MOTIVO_DESATIV,
        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("CD_MOTIVO_DESATIV") }} as string) as id_motivo_desativacao,
        cast({{ process_null("DS_MOTIVO_DESATIV") }} as string) as motivo_desativacao,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)

select *
from renamed
