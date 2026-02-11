{{
    config(
        alias="registro_civil_historico",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_HRC') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.RC_OE_GESTOR") as RC_OE_GESTOR,
        json_extract_scalar(json, "$.RC_NUM_AIH") as RC_NUM_AIH,
        json_extract_scalar(json, "$.RC_CNES") as RC_CNES,
        json_extract_scalar(json, "$.RC_CMPT") as RC_CMPT,
        json_extract_scalar(json, "$.RC_SEQ_PRINC") as RC_SEQ_PRINC,
        json_extract_scalar(json, "$.RC_INDX") as RC_INDX,
        json_extract_scalar(json, "$.RC_NUM_DEC_NASC") as RC_NUM_DEC_NASC,
        json_extract_scalar(json, "$.RC_NOME") as RC_NOME,
        json_extract_scalar(json, "$.RC_RAZSOC_CART") as RC_RAZSOC_CART,
        json_extract_scalar(json, "$.RC_LIVRO") as RC_LIVRO,
        json_extract_scalar(json, "$.RC_FOLHA") as RC_FOLHA,
        json_extract_scalar(json, "$.RC_TERMO") as RC_TERMO,
        json_extract_scalar(json, "$.RC_DT_EMISSAO_RN") as RC_DT_EMISSAO_RN,
        json_extract_scalar(json, "$.RC_LINHA") as RC_LINHA,
        json_extract_scalar(json, "$.RC_OE_REGIONAL") as RC_OE_REGIONAL,
        json_extract_scalar(json, "$.RC_MATRICULA") as RC_MATRICULA,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("RC_OE_GESTOR") }} as string) as oe_gestor,
        cast({{ process_null("RC_NUM_AIH") }} as string) as numero_aih,
        cast({{ process_null("RC_CNES") }} as string) as id_cnes,
        cast({{ process_null("RC_CMPT") }} as string) as cmpt,
        cast({{ process_null("RC_SEQ_PRINC") }} as string) as seq_princ,
        cast({{ process_null("RC_INDX") }} as string) as indx,
        cast({{ process_null("RC_NUM_DEC_NASC") }} as string) as numero_dec_nascimento,
        cast({{ process_null("trim(RC_NOME)") }} as string) as nome,
        cast({{ process_null("RC_RAZSOC_CART") }} as string) as razao_social_cart,
        
        case
            when REGEXP_CONTAINS(trim(RC_LIVRO), r"^0+$")
                then null
            else cast({{ process_null("RC_LIVRO") }} as string)
        end as livro,
        case
            when REGEXP_CONTAINS(trim(RC_FOLHA), r"^0+$")
                then null
            else cast({{ process_null("RC_FOLHA") }} as string)
        end as folha,
        case
            when REGEXP_CONTAINS(trim(RC_TERMO), r"^0+$")
                then null
            else cast({{ process_null("RC_TERMO") }} as string)
        end as termo,

        cast({{ process_null("RC_DT_EMISSAO_RN") }} as string) as data_emissao_rn,
        cast({{ process_null("RC_LINHA") }} as string) as linha,
        cast({{ process_null("RC_OE_REGIONAL") }} as string) as oe_regional,
        cast({{ process_null("RC_MATRICULA") }} as string) as matricula,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
