{{
    config(
        alias="equipe",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_EQUIPE') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.EQP_GESTOR") as EQP_GESTOR,
        json_extract_scalar(json, "$.EQP_CMP") as EQP_CMP,
        json_extract_scalar(json, "$.EQP_CNES") as EQP_CNES,
        json_extract_scalar(json, "$.EQP_AREA") as EQP_AREA,
        json_extract_scalar(json, "$.EQP_SEQ") as EQP_SEQ,
        json_extract_scalar(json, "$.EQP_MUN") as EQP_MUN,
        json_extract_scalar(json, "$.EQP_TP") as EQP_TP,
        json_extract_scalar(json, "$.EQP_NOME") as EQP_NOME,
        json_extract_scalar(json, "$.EQP_DTINI") as EQP_DTINI,
        json_extract_scalar(json, "$.EQP_DTFIM") as EQP_DTFIM,
        json_extract_scalar(json, "$.EQP_MOTFIM") as EQP_MOTFIM,
        json_extract_scalar(json, "$.EQP_TPFIM") as EQP_TPFIM,
        json_extract_scalar(json, "$.EQP_LOCNAC") as EQP_LOCNAC,
        json_extract_scalar(json, "$.EQP_INE") as EQP_INE,
        -- json_extract_scalar(json, "$.EQP_CHKSM") as EQP_CHKSM,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("EQP_GESTOR") }} as string) as gestor,
        case
            when length(trim(EQP_CMP)) = 6 and regexp_contains(trim(EQP_CMP), r'^[0-9]+$')
                then concat(left(trim(EQP_CMP), 4), '-', right(trim(EQP_CMP), 2))
            else cast({{ process_null("EQP_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("EQP_CNES") }} as string) as id_cnes,
        cast({{ process_null("EQP_AREA") }} as string) as area,
        cast({{ process_null("EQP_SEQ") }} as string) as seq,
        cast({{ process_null("EQP_MUN") }} as string) as mun,
        cast({{ process_null("EQP_TP") }} as string) as tipo,
        cast({{ process_null("trim(EQP_NOME)") }} as string) as nome,

        case
            when EQP_DTINI = "99991231"  --- 31/dez/9999
                then cast(null as string)
            else cast({{ process_null("EQP_DTINI") }} as string)
        end as data_inicio,
        case
            when EQP_DTFIM = "99991231"
                then cast(null as string)
            else cast({{ process_null("EQP_DTFIM") }} as string)
        end as data_fim,

        cast({{ process_null("trim(EQP_MOTFIM)") }} as string) as motivo_fim,
        cast({{ process_null("trim(EQP_TPFIM)") }} as string) as tipo_fim,

        cast({{ process_null("EQP_LOCNAC") }} as string) as locnac,
        cast({{ process_null("EQP_INE") }} as string) as ine,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
