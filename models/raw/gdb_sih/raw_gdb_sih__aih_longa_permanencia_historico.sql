{{
    config(
        alias="aih_longa_permanencia_historico",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_HALP') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.ALP_OE_GESTOR") as ALP_OE_GESTOR,
        json_extract_scalar(json, "$.ALP_NUM_AIH") as ALP_NUM_AIH,
        json_extract_scalar(json, "$.ALP_CNES") as ALP_CNES,
        json_extract_scalar(json, "$.ALP_EL_COD") as ALP_EL_COD,
        json_extract_scalar(json, "$.ALP_PROC_REALIZADO") as ALP_PROC_REALIZADO,
        json_extract_scalar(json, "$.ALP_DIAG_PRI") as ALP_DIAG_PRI,
        json_extract_scalar(json, "$.ALP_DT_INTERNACAO") as ALP_DT_INTERNACAO,
        json_extract_scalar(json, "$.ALP_MOT_SAIDA") as ALP_MOT_SAIDA,
        json_extract_scalar(json, "$.ALP_ULTIMA_DT_SAIDA") as ALP_ULTIMA_DT_SAIDA,
        json_extract_scalar(json, "$.ALP_OE_REGIONAL") as ALP_OE_REGIONAL,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("ALP_OE_GESTOR") }} as string) as oe_gestor,
        cast({{ process_null("ALP_NUM_AIH") }} as string) as numero_aih,
        cast({{ process_null("ALP_CNES") }} as string) as id_cnes,
        cast({{ process_null("ALP_EL_COD") }} as string) as el_codigo,
        cast({{ process_null("ALP_PROC_REALIZADO") }} as string) as procedimento_realizado,
        cast({{ process_null("ALP_DIAG_PRI") }} as string) as cid_diagnostico_principal,
        cast({{ process_null("ALP_DT_INTERNACAO") }} as string) as data_internacao,
        cast({{ process_null("ALP_MOT_SAIDA") }} as string) as motivo_saida,
        cast({{ process_null("ALP_ULTIMA_DT_SAIDA") }} as string) as ultima_data_saida,
        cast({{ process_null("ALP_OE_REGIONAL") }} as string) as oe_regional,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
