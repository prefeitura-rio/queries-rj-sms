{{
    config(
        alias="hospitais",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_HA') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.HA_OE_GESTOR") as HA_OE_GESTOR,
        json_extract_scalar(json, "$.HA_CNES") as HA_CNES,
        json_extract_scalar(json, "$.HA_CMPT_INI") as HA_CMPT_INI,
        json_extract_scalar(json, "$.HA_CMPT_FIM") as HA_CMPT_FIM,
        json_extract_scalar(json, "$.HA_RAZAO") as HA_RAZAO,
        json_extract_scalar(json, "$.HA_CNPJ") as HA_CNPJ,
        json_extract_scalar(json, "$.HA_CNPJ_MANTENEDORA") as HA_CNPJ_MANTENEDORA,
        json_extract_scalar(json, "$.HA_MN_COD") as HA_MN_COD,
        json_extract_scalar(json, "$.HA_UF_COD") as HA_UF_COD,
        json_extract_scalar(json, "$.HA_ESFERA") as HA_ESFERA,
        json_extract_scalar(json, "$.HA_RETENCAO") as HA_RETENCAO,
        json_extract_scalar(json, "$.HA_NAT_ORG") as HA_NAT_ORG,
        json_extract_scalar(json, "$.HA_NATUREZA_HOSP_SIHD") as HA_NATUREZA_HOSP_SIHD,
        json_extract_scalar(json, "$.HA_ATV_ENSINO") as HA_ATV_ENSINO,
        json_extract_scalar(json, "$.HA_STATUS_PR") as HA_STATUS_PR,
        json_extract_scalar(json, "$.HA_OE_REGIONAL") as HA_OE_REGIONAL,
        json_extract_scalar(json, "$.HA_CAPTACAO") as HA_CAPTACAO,
        json_extract_scalar(json, "$.HA_NAT_JUR") as HA_NAT_JUR,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("HA_CNES") }} as string) as id_cnes,
        cast({{ process_null("trim(HA_RAZAO)") }} as string) as razao_social,

        case
            when REGEXP_CONTAINS(trim(HA_CNPJ), r"^0+$")
                then null
            else cast({{ process_null("HA_CNPJ") }} as string)
        end as cnpj,
        case
            when REGEXP_CONTAINS(trim(HA_CNPJ_MANTENEDORA), r"^0+$")
                then null
            else cast({{ process_null("HA_CNPJ_MANTENEDORA") }} as string)
        end as cnpj_mantenedora,

        cast({{ process_null("HA_CMPT_INI") }} as string) as mes_cmpt_inicio,
        cast({{ process_null("HA_CMPT_FIM") }} as string) as mes_cmpt_fim,

        cast({{ process_null("HA_UF_COD") }} as string) as codigo_uf,
        cast({{ process_null("HA_MN_COD") }} as string) as codigo_municipio,

        cast({{ process_null("HA_OE_GESTOR") }} as string) as oe_gestor,
        cast({{ process_null("HA_OE_REGIONAL") }} as string) as oe_regional,

        -- "", "00", "01", "02", "03", "04"
        cast({{ process_null("HA_ESFERA") }} as string) as esfera,
        cast({{ process_null("HA_RETENCAO") }} as string) as retencao,
    
        cast({{ process_null("HA_NAT_JUR") }} as string) as nat_jur,
        cast({{ process_null("HA_NAT_ORG") }} as string) as nat_org,
        cast({{ process_null("HA_NATUREZA_HOSP_SIHD") }} as string) as natureza_hosp_sihd,

        -- "", "01", "03", "04", "05"
        cast({{ process_null("HA_ATV_ENSINO") }} as string) as atv_ensino,
        -- "0", "2"
        cast({{ process_null("HA_STATUS_PR") }} as string) as status_pr,
        -- "0", "1", "2"
        cast({{ process_null("HA_CAPTACAO") }} as string) as captacao,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
