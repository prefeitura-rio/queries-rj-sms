{{
    config(
        alias="codigo_descricao",
        schema= "brutos_gdb_sih",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_C_D') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.C_D_COD_TAB") as C_D_COD_TAB,
        json_extract_scalar(json, "$.C_D_COD_ITEM") as C_D_COD_ITEM,
        json_extract_scalar(json, "$.C_D_CMPT_INI") as C_D_CMPT_INI,
        json_extract_scalar(json, "$.C_D_CMPT_FIM") as C_D_CMPT_FIM,
        json_extract_scalar(json, "$.C_D_DESCRICAO") as C_D_DESCRICAO,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("C_D_COD_TAB") }} as string) as codigo_tabela,
        cast({{ process_null("C_D_COD_ITEM") }} as string) as codigo_item,
        cast({{ process_null("C_D_CMPT_INI") }} as string) as competencia_inicio,
        cast({{ process_null("C_D_CMPT_FIM") }} as string) as competencia_fim,
        cast({{ process_null("trim(C_D_DESCRICAO)") }} as string) as descricao,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
