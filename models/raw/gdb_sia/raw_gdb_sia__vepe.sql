{{
    config(
        alias="vepe",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_VEPE') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.CHAVE") as CHAVE,
        json_extract_scalar(json, "$.GESTOR") as GESTOR,
        json_extract_scalar(json, "$.CONDIC") as CONDIC,
        json_extract_scalar(json, "$.GRUPO") as GRUPO,
        json_extract_scalar(json, "$.ITEM") as ITEM,
        json_extract_scalar(json, "$.DESCRICAO") as DESCRICAO,
        json_extract_scalar(json, "$.VALOR") as VALOR,
        json_extract_scalar(json, "$.DT_MVM") as DT_MVM,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("trim(CHAVE)") }} as string) as chave,
        cast({{ process_null("GESTOR") }} as string) as gestor,
        cast({{ process_null("CONDIC") }} as string) as condic,
        cast({{ process_null("trim(GRUPO)") }} as string) as grupo,
        cast({{ process_null("trim(ITEM)") }} as string) as item,
        cast({{ process_null("DESCRICAO") }} as string) as descricao,
        cast({{ process_null("VALOR") }} as string) as valor,
        cast({{ process_null("DT_MVM") }} as string) as data_mvm,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
