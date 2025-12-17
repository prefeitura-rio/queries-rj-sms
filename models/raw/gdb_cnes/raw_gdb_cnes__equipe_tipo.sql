{{
    config(
        alias="equipe_tipo",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source("brutos_gdb_cnes_staging", "NFCES046") }}
),
extracted as (
    select
        json_extract_scalar(json, "$.TP_EQUIPE") as TP_EQUIPE,
        json_extract_scalar(json, "$.DS_EQUIPE") as DS_EQUIPE,
        json_extract_scalar(json, "$.CO_GRUPO_EQUIPE") as CO_GRUPO_EQUIPE,
        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("TP_EQUIPE") }} as string) as id_equipe_tipo,
        cast({{ process_null("DS_EQUIPE") }} as string) as equipe_descricao,
        cast({{ process_null("CO_GRUPO_EQUIPE") }} as string) as id_equipe_grupo,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
