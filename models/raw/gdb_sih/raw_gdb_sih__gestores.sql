{{
    config(
        alias="gestores",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_GESTOR') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.GE_COD_OE") as GE_COD_OE,
        json_extract_scalar(json, "$.GE_CNPJ") as GE_CNPJ,
        json_extract_scalar(json, "$.GE_NOME") as GE_NOME,
        json_extract_scalar(json, "$.GE_END") as GE_END,
        json_extract_scalar(json, "$.GE_GESTAO") as GE_GESTAO,
        json_extract_scalar(json, "$.GE_MUN_COD") as GE_MUN_COD,
        json_extract_scalar(json, "$.GE_RESPONSAVEL") as GE_RESPONSAVEL,
        json_extract_scalar(json, "$.GE_GESTOR_IDENT") as GE_GESTOR_IDENT,
        json_extract_scalar(json, "$.GE_GESTOR_DOC") as GE_GESTOR_DOC,
        json_extract_scalar(json, "$.GE_EMAIL") as GE_EMAIL,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("GE_COD_OE") }} as string) as codigo_oe,
        cast({{ process_null("GE_CNPJ") }} as string) as cnpj,
        cast({{ process_null("trim(GE_NOME)") }} as string) as nome,
        cast({{ process_null("GE_END") }} as string) as endereco,
        cast({{ process_null("GE_GESTAO") }} as string) as gestao,
        cast({{ process_null("GE_MUN_COD") }} as string) as codigo_municipio,
        cast({{ process_null("GE_RESPONSAVEL") }} as string) as responsavel,
        cast({{ process_null("GE_GESTOR_IDENT") }} as string) as gestor_ident,
        cast({{ process_null("GE_GESTOR_DOC") }} as string) as gestor_doc,
        cast({{ process_null("trim(GE_EMAIL)") }} as string) as email,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
