{{
    config(
        alias="crd",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_CRD') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.CRD_GESTOR") as CRD_GESTOR,
        json_extract_scalar(json, "$.CRD_CONDIC") as CRD_CONDIC,
        json_extract_scalar(json, "$.CRD_IN_PF") as CRD_IN_PF,
        json_extract_scalar(json, "$.CRD_CGCCPF") as CRD_CGCCPF,
        json_extract_scalar(json, "$.CRD_MVM") as CRD_MVM,
        json_extract_scalar(json, "$.CRD_BCO") as CRD_BCO,
        json_extract_scalar(json, "$.CRD_AB") as CRD_AB,
        json_extract_scalar(json, "$.CRD_CC") as CRD_CC,
        json_extract_scalar(json, "$.CRD_MN") as CRD_MN,
        json_extract_scalar(json, "$.CRD_VL") as CRD_VL,
        json_extract_scalar(json, "$.CRDPAB") as CRDPAB,
        json_extract_scalar(json, "$.CRDMEDIA") as CRDMEDIA,
        json_extract_scalar(json, "$.CRDESTRAT") as CRDESTRAT,
        json_extract_scalar(json, "$.CRDALTA") as CRDALTA,
        json_extract_scalar(json, "$.CRDBDP") as CRDBDP,
        json_extract_scalar(json, "$.CRD_IR") as CRD_IR,
        json_extract_scalar(json, "$.CRD_DESCIR") as CRD_DESCIR,
        json_extract_scalar(json, "$.CRD_VL_FED") as CRD_VL_FED,
        json_extract_scalar(json, "$.CRD_VL_LOC") as CRD_VL_LOC,
        json_extract_scalar(json, "$.CRD_VL_INC") as CRD_VL_INC,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("CRD_GESTOR") }} as string) as gestor,
        cast({{ process_null("CRD_CONDIC") }} as string) as condic,
        cast({{ process_null("CRD_IN_PF") }} as string) as in_pf,
        cast({{ process_null("CRD_CGCCPF") }} as string) as cgccpf,
        cast({{ process_null("CRD_MVM") }} as string) as mvm,
        cast({{ process_null("CRD_BCO") }} as string) as bco,
        cast({{ process_null("CRD_AB") }} as string) as ab,
        cast({{ process_null("CRD_CC") }} as string) as cc,
        cast({{ process_null("CRD_MN") }} as string) as mn,
        cast({{ process_null("CRD_VL") }} as float64) as vl,
        cast({{ process_null("CRDPAB") }} as float64) as pab,
        cast({{ process_null("CRDMEDIA") }} as float64) as media,
        cast({{ process_null("CRDESTRAT") }} as float64) as estrat,
        cast({{ process_null("CRDALTA") }} as float64) as alta,
        cast({{ process_null("CRDBDP") }} as float64) as bdp,
        cast({{ process_null("CRD_IR") }} as float64) as ir,
        cast({{ process_null("CRD_DESCIR") }} as string) as descir,
        cast({{ process_null("CRD_VL_FED") }} as float64) as valor_fed,
        cast({{ process_null("CRD_VL_LOC") }} as float64) as valor_loc,
        cast({{ process_null("CRD_VL_INC") }} as float64) as valor_inc,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
