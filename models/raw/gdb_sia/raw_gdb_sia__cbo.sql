{{
    config(
        alias="cbo",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_CBO') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.CBO_GESTOR") as CBO_GESTOR,
        json_extract_scalar(json, "$.CBO_CONDIC") as CBO_CONDIC,
        json_extract_scalar(json, "$.CBO_UID") as CBO_UID,
        json_extract_scalar(json, "$.CBO_CMP") as CBO_CMP,
        json_extract_scalar(json, "$.CBO_CBO") as CBO_CBO,
        json_extract_scalar(json, "$.CBO_QT_PR") as CBO_QT_PR,
        json_extract_scalar(json, "$.CBO_QT_HR") as CBO_QT_HR,
        json_extract_scalar(json, "$.CBO_LOCNAC") as CBO_LOCNAC,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("CBO_GESTOR") }} as string) as gestor,
        cast({{ process_null("CBO_CONDIC") }} as string) as condic,
        cast({{ process_null("CBO_UID") }} as string) as uid,
        case
            when length(trim(CBO_CMP)) = 6 and regexp_contains(trim(CBO_CMP), r'^[0-9]+$')
                then concat(left(trim(CBO_CMP), 4), '-', right(trim(CBO_CMP), 2))
            else cast({{ process_null("CBO_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("CBO_CBO") }} as string) as cbo,
        cast({{ process_null("CBO_QT_PR") }} as string) as qt_pr,
        cast({{ process_null("CBO_QT_HR") }} as string) as qt_hr,
        cast({{ process_null("CBO_LOCNAC") }} as string) as locnac,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
