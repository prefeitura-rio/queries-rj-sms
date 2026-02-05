{{
    config(
        alias="teto",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_TETO') }}
),
extracted as (
    select
        json_extract_scalar(json, "$.TE_OE_GESTOR") as TE_OE_GESTOR,
        json_extract_scalar(json, "$.TE_CNES") as TE_CNES,
        json_extract_scalar(json, "$.TE_EL_COD") as TE_EL_COD,
        json_extract_scalar(json, "$.TE_COMPLEXIDADE") as TE_COMPLEXIDADE,
        json_extract_scalar(json, "$.TE_CMPT_INI") as TE_CMPT_INI,
        json_extract_scalar(json, "$.TE_CMPT_FIM") as TE_CMPT_FIM,
        json_extract_scalar(json, "$.TE_VALOR") as TE_VALOR,
        json_extract_scalar(json, "$.TE_OE_REGIONAL") as TE_OE_REGIONAL,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("TE_OE_GESTOR") }} as string) as oe_gestor,
        cast({{ process_null("TE_CNES") }} as string) as id_cnes,
        cast({{ process_null("TE_EL_COD") }} as string) as el_codigo,
        cast({{ process_null("TE_COMPLEXIDADE") }} as string) as complexidade,
        cast({{ process_null("TE_CMPT_INI") }} as string) as data_cmpt_inicio,
        cast({{ process_null("TE_CMPT_FIM") }} as string) as data_cmpt_fim,
        cast({{ process_null("TE_VALOR") }} as string) as valor,
        cast({{ process_null("TE_OE_REGIONAL") }} as string) as oe_regional,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
