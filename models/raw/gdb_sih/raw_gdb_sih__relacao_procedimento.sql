{{
    config(
        alias="relacao_procedimento",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TU_PROCEDIMENTO') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.CO_PROCEDIMENTO") as CO_PROCEDIMENTO,
        json_extract_scalar(json, "$.NO_PROCEDIMENTO") as NO_PROCEDIMENTO,
        json_extract_scalar(json, "$.TP_COMPLEXIDADE") as TP_COMPLEXIDADE,
        json_extract_scalar(json, "$.TP_SEXO") as TP_SEXO,
        json_extract_scalar(json, "$.QT_MAXIMA_EXECUCAO") as QT_MAXIMA_EXECUCAO,
        json_extract_scalar(json, "$.QT_DIAS_PERMANENCIA") as QT_DIAS_PERMANENCIA,
        json_extract_scalar(json, "$.QT_PONTOS") as QT_PONTOS,
        json_extract_scalar(json, "$.VL_IDADE_MINIMA") as VL_IDADE_MINIMA,
        json_extract_scalar(json, "$.VL_IDADE_MAXIMA") as VL_IDADE_MAXIMA,
        json_extract_scalar(json, "$.VL_SH") as VL_SH,
        json_extract_scalar(json, "$.VL_SA") as VL_SA,
        json_extract_scalar(json, "$.VL_SP") as VL_SP,
        json_extract_scalar(json, "$.DT_INICIO_VIGENCIA") as DT_INICIO_VIGENCIA,
        json_extract_scalar(json, "$.DT_FIM_VIGENCIA") as DT_FIM_VIGENCIA,
        json_extract_scalar(json, "$.CO_FINANCIAMENTO") as CO_FINANCIAMENTO,
        json_extract_scalar(json, "$.CO_RUBRICA") as CO_RUBRICA,
        --json_extract_scalar(json, "$.TU_PROC_CS") as TU_PROC_CS,  -- checksum, descartado

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("CO_PROCEDIMENTO") }} as string) as codigo_procedimento,
        cast({{ process_null("NO_PROCEDIMENTO") }} as string) as nome_procedimento,
        cast({{ process_null("TP_COMPLEXIDADE") }} as string) as complexidade,
        cast({{ process_null("TP_SEXO") }} as string) as sexo,
        cast({{ process_null("QT_MAXIMA_EXECUCAO") }} as string) as maximo_execucoes,
        cast({{ process_null("QT_DIAS_PERMANENCIA") }} as string) as dias_permanencia,
        cast({{ process_null("QT_PONTOS") }} as string) as quantidade_pontos,
        cast({{ process_null("VL_IDADE_MINIMA") }} as string) as idade_minima,
        cast({{ process_null("VL_IDADE_MAXIMA") }} as string) as idade_maxima,
        cast({{ process_null("VL_SH") }} as string) as valor_sh,
        cast({{ process_null("VL_SA") }} as string) as valor_sa,
        cast({{ process_null("VL_SP") }} as string) as valor_sp,
        cast({{ process_null("DT_INICIO_VIGENCIA") }} as string) as data_inicio_vigencia,
        cast({{ process_null("DT_FIM_VIGENCIA") }} as string) as data_fim_vigencia,
        cast({{ process_null("CO_FINANCIAMENTO") }} as string) as codigo_financiamento,
        cast({{ process_null("CO_RUBRICA") }} as string) as codigo_rubrica,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
