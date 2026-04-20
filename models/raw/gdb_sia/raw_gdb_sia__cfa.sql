{{
    config(
        alias="cfa",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_CFA') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.CFA_CMP") as CFA_CMP,
        json_extract_scalar(json, "$.CFA_GESTOR") as CFA_GESTOR,
        json_extract_scalar(json, "$.CFA_CONDIC") as CFA_CONDIC,
        json_extract_scalar(json, "$.CFAINICIAL") as CFAINICIAL,
        json_extract_scalar(json, "$.CFAFINAL") as CFAFINAL,
        json_extract_scalar(json, "$.CFAAMINI") as CFAAMINI,
        json_extract_scalar(json, "$.CFAAMFIN") as CFAAMFIN,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        case
            when length(trim(CFA_CMP)) = 6 and regexp_contains(trim(CFA_CMP), r'^[0-9]+$')
                then concat(left(trim(CFA_CMP), 4), '-', right(trim(CFA_CMP), 2))
            else cast({{ process_null("CFA_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("CFA_GESTOR") }} as string) as gestor,
        cast({{ process_null("CFA_CONDIC") }} as string) as condic,
        cast({{ process_null("CFAINICIAL") }} as string) as cfa_inicial,
        cast({{ process_null("CFAFINAL") }} as string) as cfa_final,
        cast({{ process_null("CFAAMINI") }} as string) as am_inicio,
        cast({{ process_null("CFAAMFIN") }} as string) as am_final,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
