{{
    config(
        alias="herr",
        schema= "brutos_gdb_sih",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_HERR') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.ER_OE_GESTOR") as ER_OE_GESTOR,
        json_extract_scalar(json, "$.ER_NUM_AIH") as ER_NUM_AIH,
        json_extract_scalar(json, "$.ER_CNES") as ER_CNES,
        json_extract_scalar(json, "$.ER_CMPT") as ER_CMPT,
        json_extract_scalar(json, "$.ER_SEQ_PRINC") as ER_SEQ_PRINC,
        json_extract_scalar(json, "$.ER_COD") as ER_COD,
        json_extract_scalar(json, "$.ER_PA_INDX") as ER_PA_INDX,
        json_extract_scalar(json, "$.ER_COMPL") as ER_COMPL,
        json_extract_scalar(json, "$.ER_OE_REGIONAL") as ER_OE_REGIONAL,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("ER_OE_GESTOR") }} as string) as oe_gestor,
        cast({{ process_null("ER_NUM_AIH") }} as string) as numero_aih,
        cast({{ process_null("ER_CNES") }} as string) as cnes,
        cast({{ process_null("ER_CMPT") }} as string) as competencia,
        cast({{ process_null("ER_SEQ_PRINC") }} as string) as seq_princ,
        cast({{ process_null("ER_COD") }} as string) as codigo,
        cast({{ process_null("ER_PA_INDX") }} as string) as pa_indx,
        cast({{ process_null("ER_COMPL") }} as string) as compl,
        cast({{ process_null("ER_OE_REGIONAL") }} as string) as oe_regional,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
