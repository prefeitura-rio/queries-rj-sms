{{
    config(
        alias="tu_cid",
        schema= "brutos_gdb_sih",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TU_CID') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.CO_CID") as CO_CID,
        json_extract_scalar(json, "$.NO_CID") as NO_CID,
        json_extract_scalar(json, "$.TP_AGRAVO") as TP_AGRAVO,
        json_extract_scalar(json, "$.TP_SEXO") as TP_SEXO,
        json_extract_scalar(json, "$.TU_CID_CMPT_INI") as TU_CID_CMPT_INI,
        json_extract_scalar(json, "$.TU_CID_CMPT_FIM") as TU_CID_CMPT_FIM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        trim(cast({{ process_null("CO_CID") }} as string)) as cid_codigo,
        trim(cast({{ process_null("NO_CID") }} as string)) as cid_descricao,
        trim(cast({{ process_null("TP_AGRAVO") }} as string)) as agravo,
        trim(cast({{ process_null("TP_SEXO") }} as string)) as sexo,
        trim(cast({{ process_null("TU_CID_CMPT_INI") }} as string)) as competencia_inicio,
        trim(cast({{ process_null("TU_CID_CMPT_FIM") }} as string)) as competencia_fim,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
