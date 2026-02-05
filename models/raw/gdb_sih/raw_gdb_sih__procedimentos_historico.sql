{{
    config(
        alias="procedimentos_historico",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_HPA') }}
    tablesample system (5 percent) --fixme
),
extracted as (
    select

        json_extract_scalar(json, "$.PA_OE_GESTOR") as PA_OE_GESTOR,
        json_extract_scalar(json, "$.PA_NUM_AIH") as PA_NUM_AIH,
        json_extract_scalar(json, "$.PA_CNES") as PA_CNES,
        json_extract_scalar(json, "$.PA_CMPT") as PA_CMPT,
        json_extract_scalar(json, "$.PA_SEQ_PRINC") as PA_SEQ_PRINC,
        json_extract_scalar(json, "$.PA_INDX") as PA_INDX,
        json_extract_scalar(json, "$.PA_PF_IDENT") as PA_PF_IDENT,
        json_extract_scalar(json, "$.PA_PF_DOC") as PA_PF_DOC,
        json_extract_scalar(json, "$.PA_PF_CBO") as PA_PF_CBO,
        json_extract_scalar(json, "$.PA_PF_EQUIPE") as PA_PF_EQUIPE,
        json_extract_scalar(json, "$.PA_PJ_IDENT") as PA_PJ_IDENT,
        json_extract_scalar(json, "$.PA_PJ_DOC") as PA_PJ_DOC,
        json_extract_scalar(json, "$.PA_EXEC_IDENT") as PA_EXEC_IDENT,
        json_extract_scalar(json, "$.PA_EXEC_DOC") as PA_EXEC_DOC,
        json_extract_scalar(json, "$.PA_CREDITO_IDENT") as PA_CREDITO_IDENT,
        json_extract_scalar(json, "$.PA_CREDITO_DOC") as PA_CREDITO_DOC,
        json_extract_scalar(json, "$.PA_PROCEDIMENTO") as PA_PROCEDIMENTO,
        json_extract_scalar(json, "$.PA_PROCEDIMENTO_QTD") as PA_PROCEDIMENTO_QTD,
        json_extract_scalar(json, "$.PA_PONTO_QTD") as PA_PONTO_QTD,
        json_extract_scalar(json, "$.PA_VALOR") as PA_VALOR,
        json_extract_scalar(json, "$.PA_IND_TIPO_VALOR") as PA_IND_TIPO_VALOR,
        json_extract_scalar(json, "$.PA_IND_PRESTADOR") as PA_IND_PRESTADOR,
        json_extract_scalar(json, "$.PA_IND_RATEIO") as PA_IND_RATEIO,
        json_extract_scalar(json, "$.PA_IND_ORIGEM") as PA_IND_ORIGEM,
        json_extract_scalar(json, "$.PA_GRUPO") as PA_GRUPO,
        json_extract_scalar(json, "$.PA_SUBGRUPO") as PA_SUBGRUPO,
        json_extract_scalar(json, "$.PA_FO") as PA_FO,
        json_extract_scalar(json, "$.PA_CMPT_UTI") as PA_CMPT_UTI,
        json_extract_scalar(json, "$.PA_COMPLEXIDADE") as PA_COMPLEXIDADE,
        json_extract_scalar(json, "$.PA_FINANCIAMENTO") as PA_FINANCIAMENTO,
        json_extract_scalar(json, "$.PA_TIPO_FAEC") as PA_TIPO_FAEC,
        json_extract_scalar(json, "$.PA_CONTRATO") as PA_CONTRATO,
        json_extract_scalar(json, "$.PA_OE_REGIONAL") as PA_OE_REGIONAL,
        json_extract_scalar(json, "$.PA_SERV_CLA") as PA_SERV_CLA,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("PA_OE_GESTOR") }} as string) as oe_gestor,
        cast({{ process_null("PA_NUM_AIH") }} as string) as numero_aih,
        cast({{ process_null("PA_CNES") }} as string) as id_cnes,
        cast({{ process_null("PA_CMPT") }} as string) as cmpt,
        cast({{ process_null("PA_SEQ_PRINC") }} as string) as seq_princ,
        cast({{ process_null("PA_INDX") }} as string) as indx,

        cast({{ process_null("PA_PF_IDENT") }} as string) as pf_ident,
        case
            when REGEXP_CONTAINS(trim(PA_PF_DOC), r"^0+$")
                then null
            else cast({{ process_null("PA_PF_DOC") }} as string)
        end as pf_doc, -- documento pessoa física/profissional? = CPF/CNS?
        case
            when REGEXP_CONTAINS(trim(PA_PF_CBO), r"^0+$")
                then null
            else cast({{ process_null("PA_PF_CBO") }} as string)
        end as pf_cbo,
        cast({{ process_null("PA_PF_EQUIPE") }} as string) as pf_equipe,
    
        cast({{ process_null("PA_PJ_IDENT") }} as string) as pj_ident,
        case
            when REGEXP_CONTAINS(trim(PA_PJ_DOC), r"^0+$")
                then null
            else cast({{ process_null("PA_PJ_DOC") }} as string)
        end as pj_doc,

        cast({{ process_null("PA_EXEC_IDENT") }} as string) as exec_ident,
        case
            when REGEXP_CONTAINS(trim(PA_EXEC_DOC), r"^0+$")
                then null
            else cast({{ process_null("PA_EXEC_DOC") }} as string)
        end as exec_doc,

        cast({{ process_null("PA_CREDITO_IDENT") }} as string) as credito_ident,
        case
            when REGEXP_CONTAINS(trim(PA_CREDITO_DOC), r"^0+$")
                then null
            else cast({{ process_null("PA_CREDITO_DOC") }} as string)
        end as credito_doc,

        cast({{ process_null("PA_PROCEDIMENTO") }} as string) as procedimento,
        cast({{ process_null("PA_PROCEDIMENTO_QTD") }} as string) as procedimento_quantidade,
        cast({{ process_null("PA_PONTO_QTD") }} as string) as ponto_quantidade,
        cast({{ process_null("PA_VALOR") }} as string) as valor,
        cast({{ process_null("PA_IND_TIPO_VALOR") }} as string) as ind_tipo_valor,
        cast({{ process_null("PA_IND_PRESTADOR") }} as string) as ind_prestador,
        cast({{ process_null("PA_IND_RATEIO") }} as string) as ind_rateio,
        cast({{ process_null("PA_IND_ORIGEM") }} as string) as ind_origem,
        cast({{ process_null("PA_GRUPO") }} as string) as grupo,
        cast({{ process_null("PA_SUBGRUPO") }} as string) as subgrupo,
        cast({{ process_null("PA_FO") }} as string) as fo,
        cast({{ process_null("PA_CMPT_UTI") }} as string) as cmpt_uti,
        cast({{ process_null("PA_COMPLEXIDADE") }} as string) as complexidade,
        cast({{ process_null("PA_FINANCIAMENTO") }} as string) as financiamento,
        cast({{ process_null("PA_TIPO_FAEC") }} as string) as tipo_faec,
        cast({{ process_null("PA_CONTRATO") }} as string) as contrato,
        cast({{ process_null("PA_OE_REGIONAL") }} as string) as oe_regional,
        cast({{ process_null("PA_SERV_CLA") }} as string) as serv_cla,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
