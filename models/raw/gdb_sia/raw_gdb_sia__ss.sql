{{
    config(
        alias="ss",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_SS') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.SS_CMP") as SS_CMP,
        json_extract_scalar(json, "$.SS_UFIBGE") as SS_UFIBGE,
        json_extract_scalar(json, "$.SS_UFSIGL") as SS_UFSIGL,
        json_extract_scalar(json, "$.SS_UFNOME") as SS_UFNOME,
        json_extract_scalar(json, "$.SS_NM") as SS_NM,
        json_extract_scalar(json, "$.SS_SIGL") as SS_SIGL,
        json_extract_scalar(json, "$.SS_CGC") as SS_CGC,
        json_extract_scalar(json, "$.SS_IN") as SS_IN,
        json_extract_scalar(json, "$.SS_ABDV") as SS_ABDV,
        json_extract_scalar(json, "$.SS_CCDV") as SS_CCDV,
        json_extract_scalar(json, "$.SS_CONDIC") as SS_CONDIC,
        json_extract_scalar(json, "$.SS_GESTAO") as SS_GESTAO,
        json_extract_scalar(json, "$.SS_CONVBB") as SS_CONVBB,
        json_extract_scalar(json, "$.SS_DPCNES") as SS_DPCNES,
        json_extract_scalar(json, "$.SS_SISOPERAC") as SS_SISOPERAC,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        case
            when length(trim(SS_CMP)) = 6 and regexp_contains(trim(SS_CMP), r'^[0-9]+$')
                then concat(left(trim(SS_CMP), 4), '-', right(trim(SS_CMP), 2))
            else cast({{ process_null("SS_CMP") }} as string)
        end as mes_competencia,

        cast({{ process_null("SS_UFIBGE") }} as string) as uf_ibge,
        cast({{ process_null("SS_UFSIGL") }} as string) as uf_sigla,
        cast({{ process_null("trim(SS_UFNOME)") }} as string) as uf_nome,
        cast({{ process_null("trim(SS_NM)") }} as string) as nome,
        cast({{ process_null("trim(SS_SIGL)") }} as string) as sigla,
        cast({{ process_null("SS_CGC") }} as string) as cgc,
        cast({{ process_null("SS_IN") }} as string) as _in,
        cast({{ process_null("SS_ABDV") }} as string) as abdv,
        cast({{ process_null("SS_CCDV") }} as string) as ccdv,
        cast({{ process_null("SS_CONDIC") }} as string) as condic,
        cast({{ process_null("SS_GESTAO") }} as string) as gestao,
        cast({{ process_null("SS_CONVBB") }} as string) as convbb,
        cast({{ process_null("trim(SS_DPCNES)") }} as string) as dpcnes,
        cast({{ process_null("SS_SISOPERAC") }} as string) as sistema_operacional,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
