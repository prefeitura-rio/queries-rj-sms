{{
    config(
        alias="tu_procedimento_cid",
        schema= "brutos_gdb_sih",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TU_PROCEDIMENTO_CID') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.CO_PROCEDIMENTO") as CO_PROCEDIMENTO,
        json_extract_scalar(json, "$.CO_CID") as CO_CID,
        json_extract_scalar(json, "$.ST_PRINCIPAL") as ST_PRINCIPAL,
        json_extract_scalar(json, "$.TU_PROCCID_CMPT_INI") as TU_PROCCID_CMPT_INI,
        json_extract_scalar(json, "$.TU_PROCCID_CMPT_FIM") as TU_PROCCID_CMPT_FIM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        trim(cast({{ process_null("CO_PROCEDIMENTO") }} as string)) as procedimento_codigo,
        trim(cast({{ process_null("CO_CID") }} as string)) as cid_codigo,
        trim(cast({{ process_null("ST_PRINCIPAL") }} as string)) as st_principal,
        trim(cast({{ process_null("TU_PROCCID_CMPT_INI") }} as string)) as competencia_inicio,
        trim(cast({{ process_null("TU_PROCCID_CMPT_FIM") }} as string)) as competencia_fim,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
