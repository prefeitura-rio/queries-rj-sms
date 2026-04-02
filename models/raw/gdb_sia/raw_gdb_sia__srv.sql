{{
    config(
        alias="srv",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_SRV') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.SRV_GESTOR") as SRV_GESTOR,
        json_extract_scalar(json, "$.SRV_CONDIC") as SRV_CONDIC,
        json_extract_scalar(json, "$.SRV_UID") as SRV_UID,
        json_extract_scalar(json, "$.SRV_CMP") as SRV_CMP,
        json_extract_scalar(json, "$.SRV_SR") as SRV_SR,
        json_extract_scalar(json, "$.SRV_CSF") as SRV_CSF,
        json_extract_scalar(json, "$.SRV_LOCNAC") as SRV_LOCNAC,
        -- json_extract_scalar(json, "$.SRV_CHKSM") as SRV_CHKSM,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("SRV_GESTOR") }} as string) as gestor,
        cast({{ process_null("SRV_CONDIC") }} as string) as condic,
        cast({{ process_null("SRV_UID") }} as string) as uid,
        case
            when length(trim(SRV_CMP)) = 6 and regexp_contains(trim(SRV_CMP), r'^[0-9]+$')
                then concat(left(trim(SRV_CMP), 4), '-', right(trim(SRV_CMP), 2))
            else cast({{ process_null("SRV_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("SRV_SR") }} as string) as sr,
        cast({{ process_null("SRV_CSF") }} as string) as csf,
        cast({{ process_null("SRV_LOCNAC") }} as string) as locnac,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
