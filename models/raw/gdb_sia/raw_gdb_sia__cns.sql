{{
    config(
        alias="cns",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_CNS') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.CNS_CMP") as CNS_CMP,
        json_extract_scalar(json, "$.CNS_GESTOR") as CNS_GESTOR,
        json_extract_scalar(json, "$.CNS_CONDIC") as CNS_CONDIC,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM,
        json_extract_scalar(json, "$.CNS_DEPARA") as CNS_DEPARA,
        json_extract_scalar(json, "$.CNS_BDSIA") as CNS_BDSIA,
        json_extract_scalar(json, "$.CNS_SIA") as CNS_SIA,
        json_extract_scalar(json, "$.CNS_BDVERSIA") as CNS_BDVERSIA,
        json_extract_scalar(json, "$.CNS_MINVERSIA") as CNS_MINVERSIA,
        json_extract_scalar(json, "$.CNS_VERSIA") as CNS_VERSIA,
        json_extract_scalar(json, "$.CNS_CHAVE") as CNS_CHAVE,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        case
            when length(trim(CNS_CMP)) = 6 and regexp_contains(trim(CNS_CMP), r'^[0-9]+$')
                then concat(left(trim(CNS_CMP), 4), '-', right(trim(CNS_CMP), 2))
            else cast({{ process_null("CNS_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("CNS_GESTOR") }} as string) as gestor,
        cast({{ process_null("CNS_CONDIC") }} as string) as condic,
        cast({{ process_null("CNS_DEPARA") }} as string) as de_para,
        cast({{ process_null("CNS_BDSIA") }} as string) as bd_sia,
        cast({{ process_null("CNS_SIA") }} as string) as sia,
        cast({{ process_null("CNS_BDVERSIA") }} as string) as bd_ver_sia,
        cast({{ process_null("CNS_MINVERSIA") }} as string) as min_ver_sia,
        cast({{ process_null("CNS_VERSIA") }} as string) as ver_sia,
        cast({{ process_null("CNS_CHAVE") }} as string) as chave,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
