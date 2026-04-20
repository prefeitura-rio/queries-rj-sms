{{
    config(
        alias="upsha",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_UPSHA') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.UPSHA_CMP") as UPSHA_CMP,
        json_extract_scalar(json, "$.UPSHA_GESTOR") as UPSHA_GESTOR,
        json_extract_scalar(json, "$.UPSHA_CONDIC") as UPSHA_CONDIC,
        json_extract_scalar(json, "$.UPSHA_UPS") as UPSHA_UPS,
        json_extract_scalar(json, "$.UPSHA_HA") as UPSHA_HA,
        json_extract_scalar(json, "$.UPSHA_INI") as UPSHA_INI,
        json_extract_scalar(json, "$.UPSHA_FIM") as UPSHA_FIM,
        json_extract_scalar(json, "$.UPSHA_LOCNAC") as UPSHA_LOCNAC,
        -- json_extract_scalar(json, "$.UPSHA_CHKSM") as UPSHA_CHKSM
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        case
            when length(trim(UPSHA_CMP)) = 6 and regexp_contains(trim(UPSHA_CMP), r'^[0-9]+$')
                then concat(left(trim(UPSHA_CMP), 4), '-', right(trim(UPSHA_CMP), 2))
            else cast({{ process_null("UPSHA_CMP") }} as string)
        end as mes_competencia,

        cast({{ process_null("UPSHA_GESTOR") }} as string) as gestor,
        cast({{ process_null("UPSHA_CONDIC") }} as string) as condic,
        cast({{ process_null("UPSHA_UPS") }} as string) as ups,
        cast({{ process_null("UPSHA_HA") }} as string) as ha,
        case
            when REGEXP_CONTAINS(trim(UPSHA_INI), r"^9+$")
                then null
            else cast({{ process_null("UPSHA_INI") }} as string)
        end as mes_inicio,
        case
            when REGEXP_CONTAINS(trim(UPSHA_FIM), r"^9+$")
                then null
            else cast({{ process_null("UPSHA_FIM") }} as string)
        end as mes_fim,
        cast({{ process_null("UPSHA_LOCNAC") }} as string) as locnac,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
