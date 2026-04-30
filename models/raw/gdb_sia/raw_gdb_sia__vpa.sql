{{
    config(
        alias="vpa",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_VPA') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.VPA_GESTOR") as VPA_GESTOR,
        json_extract_scalar(json, "$.VPA_CONDIC") as VPA_CONDIC,
        json_extract_scalar(json, "$.VPA_PA") as VPA_PA,
        json_extract_scalar(json, "$.VPA_CMP") as VPA_CMP,
        json_extract_scalar(json, "$.VPA_TOTAL") as VPA_TOTAL,
        json_extract_scalar(json, "$.VPA_SP") as VPA_SP,
        json_extract_scalar(json, "$.VPA_SA") as VPA_SA,
        json_extract_scalar(json, "$.VPA_SH") as VPA_SH,
        json_extract_scalar(json, "$.VPA_MUN") as VPA_MUN,
        json_extract_scalar(json, "$.VPA_TIPO") as VPA_TIPO,
        json_extract_scalar(json, "$.VPA_CTF") as VPA_CTF,
        json_extract_scalar(json, "$.VPA_RUB") as VPA_RUB,
        json_extract_scalar(json, "$.VPA_MVM") as VPA_MVM,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        case
            when length(trim(VPA_CMP)) = 6 and regexp_contains(trim(VPA_CMP), r'^[0-9]+$')
                then concat(left(trim(VPA_CMP), 4), '-', right(trim(VPA_CMP), 2))
            else cast({{ process_null("VPA_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("VPA_GESTOR") }} as string) as gestor,
        cast({{ process_null("VPA_CONDIC") }} as string) as condic,
        cast({{ process_null("VPA_PA") }} as string) as pa,
        cast({{ process_null("VPA_TOTAL") }} as float64) as total,
        cast({{ process_null("VPA_SP") }} as float64) as sp,
        cast({{ process_null("VPA_SA") }} as float64) as sa,
        cast({{ process_null("VPA_SH") }} as float64) as sh,
        cast({{ process_null("VPA_MUN") }} as string) as municipio,
        cast({{ process_null("VPA_TIPO") }} as string) as tipo,
        cast({{ process_null("VPA_CTF") }} as string) as ctf,
        cast({{ process_null("VPA_RUB") }} as string) as rub,
        cast({{ process_null("VPA_MVM") }} as string) as mvm,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
