{{
    config(
        alias="encerramento",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_ENCERRAMENTO') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.DT_MVM") as DT_MVM,
        json_extract_scalar(json, "$.GESTOR") as GESTOR,
        json_extract_scalar(json, "$.CONDIC") as CONDIC,
        json_extract_scalar(json, "$.CD_BDSIA") as CD_BDSIA,
        json_extract_scalar(json, "$.CD_SIASUS") as CD_SIASUS,
        json_extract_scalar(json, "$.DT_TDTINI") as DT_TDTINI,
        json_extract_scalar(json, "$.HR_THRINI") as HR_THRINI,
        json_extract_scalar(json, "$.DT_TDTFIM") as DT_TDTFIM,
        json_extract_scalar(json, "$.HR_THRFIM") as HR_THRFIM,
        json_extract_scalar(json, "$.DT_CDTINI") as DT_CDTINI,
        json_extract_scalar(json, "$.HR_CHRINI") as HR_CHRINI,
        json_extract_scalar(json, "$.DT_CDTFIM") as DT_CDTFIM,
        json_extract_scalar(json, "$.HR_CHRFIM") as HR_CHRFIM,
        json_extract_scalar(json, "$.DT_BDTINI") as DT_BDTINI,
        json_extract_scalar(json, "$.HR_BHRINI") as HR_BHRINI,
        json_extract_scalar(json, "$.DT_BDTFIM") as DT_BDTFIM,
        json_extract_scalar(json, "$.HR_BHRFIM") as HR_BHRFIM,
        json_extract_scalar(json, "$.DT_VDTINI") as DT_VDTINI,
        json_extract_scalar(json, "$.HR_VHRINI") as HR_VHRINI,
        json_extract_scalar(json, "$.DT_VDTFIM") as DT_VDTFIM,
        json_extract_scalar(json, "$.HR_VHRFIM") as HR_VHRFIM,
        json_extract_scalar(json, "$.DT_RDTINI") as DT_RDTINI,
        json_extract_scalar(json, "$.HR_RHRINI") as HR_RHRINI,
        json_extract_scalar(json, "$.DT_RDTFIM") as DT_RDTFIM,
        json_extract_scalar(json, "$.HR_RHRFIM") as HR_RHRFIM,
        json_extract_scalar(json, "$.TT_TTOTH") as TT_TTOTH,
        json_extract_scalar(json, "$.TT_CTOTH") as TT_CTOTH,
        json_extract_scalar(json, "$.TT_BTOTH") as TT_BTOTH,
        json_extract_scalar(json, "$.TT_VTOTH") as TT_VTOTH,
        json_extract_scalar(json, "$.TT_RTOTH") as TT_RTOTH,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("DT_MVM") }} as string) as data_mvm,
        cast({{ process_null("GESTOR") }} as string) as gestor,
        cast({{ process_null("CONDIC") }} as string) as condic,
        cast({{ process_null("CD_BDSIA") }} as string) as bdsia,
        cast({{ process_null("CD_SIASUS") }} as string) as siasus,
        cast({{ process_null("DT_TDTINI") }} as string) as t_data_inicio,
        cast({{ process_null("HR_THRINI") }} as string) as t_hora_inicio,
        cast({{ process_null("DT_TDTFIM") }} as string) as t_data_fim,
        cast({{ process_null("HR_THRFIM") }} as string) as t_hora_fim,
        cast({{ process_null("DT_CDTINI") }} as string) as c_data_inicio,
        cast({{ process_null("HR_CHRINI") }} as string) as c_hora_inicio,
        cast({{ process_null("DT_CDTFIM") }} as string) as c_data_fim,
        cast({{ process_null("HR_CHRFIM") }} as string) as c_hora_fim,
        cast({{ process_null("DT_BDTINI") }} as string) as b_data_inicio,
        cast({{ process_null("HR_BHRINI") }} as string) as b_hora_inicio,
        cast({{ process_null("DT_BDTFIM") }} as string) as b_data_fim,
        cast({{ process_null("HR_BHRFIM") }} as string) as b_hora_fim,
        cast({{ process_null("DT_VDTINI") }} as string) as v_data_inicio,
        cast({{ process_null("HR_VHRINI") }} as string) as v_hora_inicio,
        cast({{ process_null("DT_VDTFIM") }} as string) as v_data_fim,
        cast({{ process_null("HR_VHRFIM") }} as string) as v_hora_fim,
        cast({{ process_null("DT_RDTINI") }} as string) as r_data_inicio,
        cast({{ process_null("HR_RHRINI") }} as string) as r_hora_inicio,
        cast({{ process_null("DT_RDTFIM") }} as string) as r_data_fim,
        cast({{ process_null("HR_RHRFIM") }} as string) as r_hora_fim,
        cast({{ process_null("TT_TTOTH") }} as string) as t_toth,
        cast({{ process_null("TT_CTOTH") }} as string) as c_toth,
        cast({{ process_null("TT_BTOTH") }} as string) as b_toth,
        cast({{ process_null("TT_VTOTH") }} as string) as v_toth,
        cast({{ process_null("TT_RTOTH") }} as string) as r_toth,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
