{{
    config(
        alias="ups",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_UPS') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.UPS_CMP") as UPS_CMP,
        json_extract_scalar(json, "$.UPS_GESTOR") as UPS_GESTOR,
        json_extract_scalar(json, "$.UPS_CONDIC") as UPS_CONDIC,
        json_extract_scalar(json, "$.UPS_ID") as UPS_ID,
        json_extract_scalar(json, "$.UPS_UF") as UPS_UF,
        json_extract_scalar(json, "$.UPS_RZSC") as UPS_RZSC,
        json_extract_scalar(json, "$.UPS_NMFN") as UPS_NMFN,
        json_extract_scalar(json, "$.UPS_IN_ATV") as UPS_IN_ATV,
        json_extract_scalar(json, "$.UPS_IN_FJ") as UPS_IN_FJ,
        json_extract_scalar(json, "$.UPS_CGCCPF") as UPS_CGCCPF,
        json_extract_scalar(json, "$.UPS_IR") as UPS_IR,
        json_extract_scalar(json, "$.UPS_LOGR") as UPS_LOGR,
        json_extract_scalar(json, "$.UPS_NUM") as UPS_NUM,
        json_extract_scalar(json, "$.UPS_COMPL") as UPS_COMPL,
        json_extract_scalar(json, "$.UPS_BAIRRO") as UPS_BAIRRO,
        json_extract_scalar(json, "$.UPS_DDD") as UPS_DDD,
        json_extract_scalar(json, "$.UPS_TELE") as UPS_TELE,
        json_extract_scalar(json, "$.UPS_CEP") as UPS_CEP,
        json_extract_scalar(json, "$.UPS_MN") as UPS_MN,
        json_extract_scalar(json, "$.UPS_DS") as UPS_DS,
        json_extract_scalar(json, "$.UPS_RS") as UPS_RS,
        json_extract_scalar(json, "$.UPS_AB") as UPS_AB,
        json_extract_scalar(json, "$.UPS_NU_CC") as UPS_NU_CC,
        json_extract_scalar(json, "$.UPS_IN_MN") as UPS_IN_MN,
        json_extract_scalar(json, "$.UPS_NU_CT") as UPS_NU_CT,
        json_extract_scalar(json, "$.UPS_DT_IN") as UPS_DT_IN,
        json_extract_scalar(json, "$.UPS_DT_CT") as UPS_DT_CT,
        json_extract_scalar(json, "$.UPS_DT_PR") as UPS_DT_PR,
        json_extract_scalar(json, "$.UPS_TUP") as UPS_TUP,
        json_extract_scalar(json, "$.UPS_TP") as UPS_TP,
        json_extract_scalar(json, "$.UPS_QT_CM") as UPS_QT_CM,
        json_extract_scalar(json, "$.UPS_QT_EO") as UPS_QT_EO,
        json_extract_scalar(json, "$.UPS_QT_SG") as UPS_QT_SG,
        json_extract_scalar(json, "$.UPS_QT_SPC") as UPS_QT_SPC,
        json_extract_scalar(json, "$.UPS_QT_SCA") as UPS_QT_SCA,
        json_extract_scalar(json, "$.UPS_TA") as UPS_TA,
        json_extract_scalar(json, "$.UPS_FC") as UPS_FC,
        json_extract_scalar(json, "$.UPS_CNES") as UPS_CNES,
        json_extract_scalar(json, "$.UPS_IDANT") as UPS_IDANT,
        json_extract_scalar(json, "$.UPS_NH") as UPS_NH,
        json_extract_scalar(json, "$.UPSATUCNES") as UPSATUCNES,
        json_extract_scalar(json, "$.UPS_BANCO") as UPS_BANCO,
        json_extract_scalar(json, "$.UPS_MNT") as UPS_MNT,
        json_extract_scalar(json, "$.UPS_LOCNAC") as UPS_LOCNAC,
        json_extract_scalar(json, "$.UPS_NATJUR") as UPS_NATJUR,
        -- json_extract_scalar(json, "$.UPS_CHKSM") as UPS_CHKSM
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        case
            when length(trim(UPS_CMP)) = 6 and regexp_contains(trim(UPS_CMP), r'^[0-9]+$')
                then concat(left(trim(UPS_CMP), 4), '-', right(trim(UPS_CMP), 2))
            else cast({{ process_null("UPS_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("UPS_GESTOR") }} as string) as gestor,
        cast({{ process_null("UPS_CONDIC") }} as string) as condic,
        cast({{ process_null("UPS_ID") }} as string) as id,
        cast({{ process_null("UPS_CNES") }} as string) as id_cnes,
        cast({{ process_null("trim(UPS_RZSC)") }} as string) as razao_social,
        cast({{ process_null("trim(UPS_NMFN)") }} as string) as nome_fantasia,
        cast({{ process_null("UPS_UF") }} as string) as endereco_uf,
        cast({{ process_null("UPS_MN") }} as string) as endereco_municipio,
        cast({{ process_null("UPS_BAIRRO") }} as string) as endereco_bairro,
        cast({{ process_null("UPS_LOGR") }} as string) as endereco_logradouro,
        cast({{ process_null("UPS_NUM") }} as string) as endereco_numero,
        cast({{ process_null("UPS_COMPL") }} as string) as endereco_complemento,
        cast({{ process_null("UPS_CEP") }} as string) as endereco_cep,
        cast({{ process_null("UPS_DDD") }} as string) as telefone_ddd,
        cast({{ process_null("UPS_TELE") }} as string) as telefone_numero,
        cast({{ process_null("UPS_IN_ATV") }} as string) as in_atv,
        cast({{ process_null("UPS_IN_FJ") }} as string) as in_fj,
        cast({{ process_null("UPS_CGCCPF") }} as string) as cgccpf,
        cast({{ process_null("UPS_IR") }} as string) as ir,
        cast({{ process_null("UPS_DS") }} as string) as ds,
        cast({{ process_null("UPS_RS") }} as string) as rs,
        cast({{ process_null("UPS_AB") }} as string) as ab,
        cast({{ process_null("UPS_NU_CC") }} as string) as numero_conta_corrente,
        cast({{ process_null("UPS_BANCO") }} as string) as banco,
        cast({{ process_null("UPS_IN_MN") }} as string) as in_mn,
        cast({{ process_null("UPS_NU_CT") }} as string) as numero_ct,
        cast({{ process_null("UPS_DT_IN") }} as string) as data_in,
        cast({{ process_null("UPS_DT_CT") }} as string) as data_ct, -- contrato?
        cast({{ process_null("UPS_DT_PR") }} as string) as data_pr,
        cast({{ process_null("UPS_TUP") }} as string) as tup,
        cast({{ process_null("UPS_TP") }} as string) as tp,
        cast({{ process_null("UPS_QT_CM") }} as string) as quantidade_cm,
        cast({{ process_null("UPS_QT_EO") }} as string) as quantidade_eo,
        cast({{ process_null("UPS_QT_SG") }} as string) as quantidade_sg,
        cast({{ process_null("UPS_QT_SPC") }} as string) as quantidade_spc,
        cast({{ process_null("UPS_QT_SCA") }} as string) as quantidade_sca,
        cast({{ process_null("UPS_TA") }} as string) as ta,
        cast({{ process_null("UPS_FC") }} as string) as fc,
        cast({{ process_null("UPS_IDANT") }} as string) as idant,
        cast({{ process_null("UPS_NH") }} as string) as nh,
        cast({{ process_null("UPSATUCNES") }} as string) as atucnes,
        cast({{ process_null("UPS_MNT") }} as string) as mnt,
        cast({{ process_null("UPS_LOCNAC") }} as string) as locnac,
        cast({{ process_null("UPS_NATJUR") }} as string) as natjur,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
