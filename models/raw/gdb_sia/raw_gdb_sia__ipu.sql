{{
    config(
        alias="ipu",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_IPU') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.IPU_GESTOR") as IPU_GESTOR,
        json_extract_scalar(json, "$.IPU_CONDIC") as IPU_CONDIC,
        json_extract_scalar(json, "$.IPU_UID") as IPU_UID,
        json_extract_scalar(json, "$.IPU_CMP") as IPU_CMP,
        json_extract_scalar(json, "$.IPU_TPFIN") as IPU_TPFIN,
        json_extract_scalar(json, "$.IPU_PA") as IPU_PA,
        json_extract_scalar(json, "$.IPU_NAPU") as IPU_NAPU,
        json_extract_scalar(json, "$.IPU_QT_O") as IPU_QT_O,
        json_extract_scalar(json, "$.IPU_VU_O") as IPU_VU_O,
        json_extract_scalar(json, "$.IPU_VL_O") as IPU_VL_O,
        json_extract_scalar(json, "$.IPU_QT_P") as IPU_QT_P,
        json_extract_scalar(json, "$.IPU_VL_P") as IPU_VL_P,
        json_extract_scalar(json, "$.IPU_QT_A") as IPU_QT_A,
        json_extract_scalar(json, "$.IPU_VL_A") as IPU_VL_A,
        json_extract_scalar(json, "$.IPU_VLAEST") as IPU_VLAEST,
        json_extract_scalar(json, "$.IPU_VLPEST") as IPU_VLPEST,
        json_extract_scalar(json, "$.IPU_VLOE") as IPU_VLOE,
        json_extract_scalar(json, "$.IPU_VL_J") as IPU_VL_J,
        json_extract_scalar(json, "$.IPU_MVM") as IPU_MVM,
        json_extract_scalar(json, "$.IPU_FPOMAG") as IPU_FPOMAG,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("IPU_GESTOR") }} as string) as gestor,
        cast({{ process_null("IPU_CONDIC") }} as string) as condic,
        cast({{ process_null("IPU_UID") }} as string) as uid,
        case
            when length(trim(IPU_CMP)) = 6 and regexp_contains(trim(IPU_CMP), r'^[0-9]+$')
                then concat(left(trim(IPU_CMP), 4), '-', right(trim(IPU_CMP), 2))
            else cast({{ process_null("IPU_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("IPU_TPFIN") }} as string) as tpfin, -- tipo finalidade?
        cast({{ process_null("IPU_PA") }} as string) as pa,
        cast({{ process_null("IPU_NAPU") }} as string) as napu,
        cast({{ process_null("IPU_QT_O") }} as int64) as o_qt,
        cast({{ process_null("IPU_VU_O") }} as float64) as o_vu, -- valor unitário?
        cast({{ process_null("IPU_VL_O") }} as float64) as o_vl,
        cast({{ process_null("IPU_QT_P") }} as int64) as p_qt,
        cast({{ process_null("IPU_VL_P") }} as float64) as p_vl,
        cast({{ process_null("IPU_QT_A") }} as int64) as a_qt,
        cast({{ process_null("IPU_VL_A") }} as float64) as a_vl,
        cast({{ process_null("IPU_VLAEST") }} as float64) as a_vl_est, -- valor estimado?
        cast({{ process_null("IPU_VLPEST") }} as float64) as p_vl_est,
        cast({{ process_null("IPU_VLOE") }} as float64) as o_vl_e,
        cast({{ process_null("IPU_VL_J") }} as float64) as j_vl,
        cast({{ process_null("trim(IPU_MVM)") }} as string) as mvm,
        cast({{ process_null("IPU_FPOMAG") }} as string) as fpomag,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
