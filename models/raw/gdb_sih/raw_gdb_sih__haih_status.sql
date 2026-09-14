{{
    config(
        alias="haih_status",
        schema= "brutos_gdb_sih",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_HAIH_STATUS') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.AIH_ST_OE_GESTOR") as AIH_ST_OE_GESTOR,
        json_extract_scalar(json, "$.AIH_ST_NUM_AIH") as AIH_ST_NUM_AIH,
        json_extract_scalar(json, "$.AIH_ST_CMPT_PROCESSAMENTO") as AIH_ST_CMPT_PROCESSAMENTO,
        json_extract_scalar(json, "$.AIH_ST_CMPT") as AIH_ST_CMPT,
        json_extract_scalar(json, "$.AIH_ST_CNES") as AIH_ST_CNES,
        json_extract_scalar(json, "$.AIH_ST_STATUS") as AIH_ST_STATUS,
        json_extract_scalar(json, "$.AIH_ST_OE_REGIONAL") as AIH_ST_OE_REGIONAL,
        json_extract_scalar(json, "$.AIH_ST_PRIM_CMPT") as AIH_ST_PRIM_CMPT,
        json_extract_scalar(json, "$.AIH_ST_PARCELA") as AIH_ST_PARCELA,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("AIH_ST_OE_GESTOR") }} as string) as emissor_gestor,
        cast({{ process_null("AIH_ST_NUM_AIH") }} as string) as numero_aih,
        cast({{ process_null("AIH_ST_CMPT_PROCESSAMENTO") }} as string) as competencia_processamento,
        cast({{ process_null("AIH_ST_CMPT") }} as string) as competencia,
        cast({{ process_null("AIH_ST_CNES") }} as string) as id_cnes,
        cast({{ process_null("AIH_ST_STATUS") }} as string) as status,
        cast({{ process_null("AIH_ST_OE_REGIONAL") }} as string) as emissor_regional,
        cast({{ process_null("AIH_ST_PRIM_CMPT") }} as string) as prim_competencia,
        cast({{ process_null("AIH_ST_PARCELA") }} as string) as parcela,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
