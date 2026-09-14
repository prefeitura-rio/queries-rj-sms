{{
    config(
        alias="faixa_aih",
        schema= "brutos_gdb_sih",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_FAIXA_AIH') }}
),
extracted as (
    select
        json_extract_scalar(json, "$.FA_OE_GESTOR") as FA_OE_GESTOR,
        json_extract_scalar(json, "$.FA_AIH_INI") as FA_AIH_INI,
        json_extract_scalar(json, "$.FA_AIH_FIM") as FA_AIH_FIM,
        json_extract_scalar(json, "$.FA_CMPT_INI") as FA_CMPT_INI,
        json_extract_scalar(json, "$.FA_CMPT_FIM") as FA_CMPT_FIM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("FA_OE_GESTOR") }} as string) as emissor_gestor,
        cast({{ process_null("FA_AIH_INI") }} as string) as faixa_inicio,
        cast({{ process_null("FA_AIH_FIM") }} as string) as faixa_fim,
        cast({{ process_null("FA_CMPT_INI") }} as string) as competencia_inicio,
        cast({{ process_null("FA_CMPT_FIM") }} as string) as competencia_fim,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
