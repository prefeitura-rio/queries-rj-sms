{{
    config(
        alias="cadastro_terceiros",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_TC') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.TC_OE_GESTOR") as TC_OE_GESTOR,
        json_extract_scalar(json, "$.TC_IDENT") as TC_IDENT,
        json_extract_scalar(json, "$.TC_DOC") as TC_DOC,
        json_extract_scalar(json, "$.TC_CMPT_INI") as TC_CMPT_INI,
        json_extract_scalar(json, "$.TC_CMPT_FIM") as TC_CMPT_FIM,
        json_extract_scalar(json, "$.TC_DESCRICAO") as TC_DESCRICAO,
        json_extract_scalar(json, "$.TC_IND_ORIGEM") as TC_IND_ORIGEM,
        json_extract_scalar(json, "$.TC_TIPO") as TC_TIPO,
        json_extract_scalar(json, "$.TC_CMPT_ACERTO") as TC_CMPT_ACERTO,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("TC_OE_GESTOR") }} as string) as oe_gestor,
        cast({{ process_null("TC_IDENT") }} as string) as ident,
        cast({{ process_null("TC_DOC") }} as string) as doc,
        cast({{ process_null("TC_CMPT_INI") }} as string) as data_cmpt_inicio,
        cast({{ process_null("TC_CMPT_FIM") }} as string) as data_cmpt_fim,
        cast({{ process_null("trim(TC_DESCRICAO)") }} as string) as descricao,
        cast({{ process_null("TC_IND_ORIGEM") }} as string) as ind_origem,
        cast({{ process_null("TC_TIPO") }} as string) as tipo,
        cast({{ process_null("TC_CMPT_ACERTO") }} as string) as cmpt_acerto,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
