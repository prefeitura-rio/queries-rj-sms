{{
    config(
        alias="mnt",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_MNT') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.MNT_CMP") as MNT_CMP,
        json_extract_scalar(json, "$.MNT_GESTOR") as MNT_GESTOR,
        json_extract_scalar(json, "$.MNT_CONDIC") as MNT_CONDIC,
        json_extract_scalar(json, "$.MNT_ID") as MNT_ID,
        json_extract_scalar(json, "$.MNT_CGCFI") as MNT_CGCFI,
        json_extract_scalar(json, "$.MNT_CGCDV") as MNT_CGCDV,
        json_extract_scalar(json, "$.MNT_IR") as MNT_IR,
        json_extract_scalar(json, "$.MNT_LOGR") as MNT_LOGR,
        json_extract_scalar(json, "$.MNT_NUM") as MNT_NUM,
        json_extract_scalar(json, "$.MNT_COMPL") as MNT_COMPL,
        json_extract_scalar(json, "$.MNT_BAIRRO") as MNT_BAIRRO,
        json_extract_scalar(json, "$.MNT_CEP") as MNT_CEP,
        json_extract_scalar(json, "$.MNT_DDD") as MNT_DDD,
        json_extract_scalar(json, "$.MNT_TELE") as MNT_TELE,
        json_extract_scalar(json, "$.MNT_DT_PR") as MNT_DT_PR,
        json_extract_scalar(json, "$.MNT_DT_IN") as MNT_DT_IN,
        json_extract_scalar(json, "$.MNT_DT_CT") as MNT_DT_CT,
        json_extract_scalar(json, "$.MNT_DT_UA") as MNT_DT_UA,
        json_extract_scalar(json, "$.MNT_RZSC") as MNT_RZSC,
        json_extract_scalar(json, "$.MNT_AB") as MNT_AB,
        json_extract_scalar(json, "$.MNT_NU_CC") as MNT_NU_CC,
        json_extract_scalar(json, "$.MNT_MN") as MNT_MN,
        json_extract_scalar(json, "$.MNT_RS") as MNT_RS,
        json_extract_scalar(json, "$.MNT_TPCC") as MNT_TPCC,
        json_extract_scalar(json, "$.MNT_BANCO") as MNT_BANCO,
        json_extract_scalar(json, "$.MNT_LOCNAC") as MNT_LOCNAC,
        -- json_extract_scalar(json, "$.MNT_CHKSM") as MNT_CHKSM,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        case
            when length(trim(MNT_CMP)) = 6 and regexp_contains(trim(MNT_CMP), r'^[0-9]+$')
                then concat(left(trim(MNT_CMP), 4), '-', right(trim(MNT_CMP), 2))
            else cast({{ process_null("MNT_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("MNT_GESTOR") }} as string) as gestor,
        cast({{ process_null("MNT_CONDIC") }} as string) as condic,
        cast({{ process_null("MNT_ID") }} as string) as id,
        cast({{ process_null("MNT_CGCFI") }} as string) as cgcfi,
        cast({{ process_null("MNT_CGCDV") }} as string) as cgcdv,
        cast({{ process_null("MNT_IR") }} as string) as ir,
        cast({{ process_null("trim(MNT_LOGR)") }} as string) as endereco_logradouro,
        cast({{ process_null("trim(MNT_NUM)") }} as string) as endereco_numero,
        cast({{ process_null("trim(MNT_COMPL)") }} as string) as endereco_complemento,
        cast({{ process_null("trim(MNT_BAIRRO)") }} as string) as endereco_bairro,
        cast({{ process_null("trim(MNT_CEP)") }} as string) as endereco_cep,
        cast({{ process_null("trim(MNT_DDD)") }} as string) as telefone_ddd,
        cast({{ process_null("trim(MNT_TELE)") }} as string) as telefone_numero,
        cast({{ process_null("MNT_DT_PR") }} as string) as data_pr,
        cast({{ process_null("MNT_DT_IN") }} as string) as data_in,
        cast({{ process_null("MNT_DT_CT") }} as string) as data_ct,
        cast({{ process_null("MNT_DT_UA") }} as string) as data_ua,
        cast({{ process_null("trim(MNT_RZSC)") }} as string) as razao_social,
        cast({{ process_null("MNT_AB") }} as string) as ab,
        cast({{ process_null("MNT_NU_CC") }} as string) as numero_conta,
        cast({{ process_null("MNT_MN") }} as string) as mn,
        cast({{ process_null("MNT_RS") }} as string) as rs,
        cast({{ process_null("MNT_TPCC") }} as string) as tipo_conta,
        cast({{ process_null("MNT_BANCO") }} as string) as banco,
        cast({{ process_null("MNT_LOCNAC") }} as string) as locnac,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
