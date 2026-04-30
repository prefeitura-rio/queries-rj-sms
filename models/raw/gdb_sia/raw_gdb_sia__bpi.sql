{{
    config(
        alias="bpi",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_BPI') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.BPI_GESTOR") as BPI_GESTOR,
        json_extract_scalar(json, "$.BPI_CONDIC") as BPI_CONDIC,
        json_extract_scalar(json, "$.BPI_UID") as BPI_UID,
        json_extract_scalar(json, "$.BPI_CMP") as BPI_CMP,
        json_extract_scalar(json, "$.BPI_CNSMED") as BPI_CNSMED,
        json_extract_scalar(json, "$.BPI_CBO") as BPI_CBO,
        json_extract_scalar(json, "$.BPI_FLH") as BPI_FLH,
        json_extract_scalar(json, "$.BPI_SEQ") as BPI_SEQ,
        json_extract_scalar(json, "$.BPI_PA") as BPI_PA,
        json_extract_scalar(json, "$.BPI_CNSPAC") as BPI_CNSPAC,
        json_extract_scalar(json, "$.BPI_CPFPCT") as BPI_CPFPCT,
        json_extract_scalar(json, "$.BPI_NMPAC") as BPI_NMPAC,
        json_extract_scalar(json, "$.BPI_DTNASC") as BPI_DTNASC,
        json_extract_scalar(json, "$.BPI_SEXO") as BPI_SEXO,
        json_extract_scalar(json, "$.BPI_IBGE") as BPI_IBGE,
        json_extract_scalar(json, "$.BPI_DTATEN") as BPI_DTATEN,
        json_extract_scalar(json, "$.BPI_CID") as BPI_CID,
        json_extract_scalar(json, "$.BPI_CATEN") as BPI_CATEN,
        json_extract_scalar(json, "$.BPI_NAUT") as BPI_NAUT,
        json_extract_scalar(json, "$.BPI_QT_P") as BPI_QT_P,
        json_extract_scalar(json, "$.BPI_QT_A") as BPI_QT_A,
        json_extract_scalar(json, "$.BPI_IDADE") as BPI_IDADE,
        json_extract_scalar(json, "$.BPI_MVM") as BPI_MVM,
        json_extract_scalar(json, "$.BPI_ORG") as BPI_ORG,
        json_extract_scalar(json, "$.BPI_TPFIN") as BPI_TPFIN,
        json_extract_scalar(json, "$.BPI_RMS") as BPI_RMS,
        json_extract_scalar(json, "$.BPI_FLPA") as BPI_FLPA,
        json_extract_scalar(json, "$.BPI_FLCID") as BPI_FLCID,
        json_extract_scalar(json, "$.BPI_FLCBO") as BPI_FLCBO,
        json_extract_scalar(json, "$.BPI_FLCA") as BPI_FLCA,
        json_extract_scalar(json, "$.BPI_FLIDA") as BPI_FLIDA,
        json_extract_scalar(json, "$.BPI_FLQT") as BPI_FLQT,
        json_extract_scalar(json, "$.BPI_FLER") as BPI_FLER,
        json_extract_scalar(json, "$.BPI_RACA") as BPI_RACA,
        json_extract_scalar(json, "$.BPI_ETNIA") as BPI_ETNIA,
        json_extract_scalar(json, "$.BPI_NACIO") as BPI_NACIO,
        json_extract_scalar(json, "$.BPI_SRV") as BPI_SRV,
        json_extract_scalar(json, "$.BPI_CSF") as BPI_CSF,
        json_extract_scalar(json, "$.BPI_EQUIP") as BPI_EQUIP,
        json_extract_scalar(json, "$.BPI_EQP_AREA") as BPI_EQP_AREA,
        json_extract_scalar(json, "$.BPI_EQP_SEQ") as BPI_EQP_SEQ,
        json_extract_scalar(json, "$.BPI_CNPJ") as BPI_CNPJ,
        json_extract_scalar(json, "$.BPI_CEPPCN") as BPI_CEPPCN,
        json_extract_scalar(json, "$.BPI_COD_LOGR") as BPI_COD_LOGR,
        json_extract_scalar(json, "$.BPI_LOGPCN") as BPI_LOGPCN,
        json_extract_scalar(json, "$.BPI_CPLPCN") as BPI_CPLPCN,
        json_extract_scalar(json, "$.BPI_NUMPCN") as BPI_NUMPCN,
        json_extract_scalar(json, "$.BPI_BAIRRO") as BPI_BAIRRO,
        json_extract_scalar(json, "$.BPI_DDD") as BPI_DDD,
        json_extract_scalar(json, "$.BPI_TEL") as BPI_TEL,
        json_extract_scalar(json, "$.BPI_EMAIL") as BPI_EMAIL,
        json_extract_scalar(json, "$.BPI_INE") as BPI_INE,
        json_extract_scalar(json, "$.BPI_ADVSEX") as BPI_ADVSEX,
        json_extract_scalar(json, "$.BPI_STRUA") as BPI_STRUA,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("BPI_GESTOR") }} as string) as gestor,
        cast({{ process_null("BPI_CONDIC") }} as string) as condic,
        cast({{ process_null("BPI_UID") }} as string) as uid,

        case
            when length(trim(BPI_CMP)) = 6 and regexp_contains(trim(BPI_CMP), r'^[0-9]+$')
                then concat(left(trim(BPI_CMP), 4), '-', right(trim(BPI_CMP), 2))
            else cast({{ process_null("BPI_CMP") }} as string)
        end as mes_competencia,

        cast({{ process_null("BPI_CNSMED") }} as string) as medico_cns,
        cast({{ process_null("BPI_CBO") }} as string) as cbo,
        cast({{ process_null("BPI_FLH") }} as string) as flh,
        cast({{ process_null("BPI_SEQ") }} as string) as seq,
        cast({{ process_null("BPI_PA") }} as string) as pa,

        case
            when REGEXP_CONTAINS(trim(BPI_CNSPAC), r"^0+$")
                then null
            else cast({{ process_null("BPI_CNSPAC") }} as string)
        end as paciente_cns,
        case
            when REGEXP_CONTAINS(trim(BPI_CPFPCT), r"^0+$")
                then null
            else cast({{ process_null("BPI_CPFPCT") }} as string)
        end as paciente_cpf,

        cast({{ process_null("trim(BPI_NMPAC)") }} as string) as paciente_nome,
    
        safe.parse_date("%Y%m%d", {{ process_null("BPI_DTNASC") }}) as data_nascimento,

        case lower(trim(BPI_SEXO))
            when 'm' then 'masculino'
            when 'f' then 'feminino'
            else cast({{ process_null("BPI_SEXO") }} as string)
        end as sexo,

        cast({{ process_null("BPI_IBGE") }} as string) as ibge,

        safe.parse_date("%Y%m%d", {{ process_null("BPI_DTATEN") }}) as data_atendimento, --?

        cast({{ process_null("BPI_CID") }} as string) as cid,
        cast({{ process_null("BPI_CATEN") }} as string) as carater_atendimento,
        cast({{ process_null("BPI_NAUT") }} as string) as numero_autorizacao,
        cast({{ process_null("BPI_QT_P") }} as string) as qt_p, -- quantidade de pacientes/atendimentos?
        cast({{ process_null("BPI_QT_A") }} as string) as qt_a,
        cast({{ process_null("BPI_IDADE") }} as int64) as idade,
        cast({{ process_null("BPI_MVM") }} as string) as mvm, -- parece ser um mês
        cast({{ process_null("BPI_ORG") }} as string) as org,
        cast({{ process_null("BPI_TPFIN") }} as string) as tpfin,
        cast({{ process_null("BPI_RMS") }} as string) as rms,
        cast({{ process_null("BPI_FLPA") }} as string) as flpa,
        cast({{ process_null("BPI_FLCID") }} as string) as flcid,
        cast({{ process_null("BPI_FLCBO") }} as string) as flcbo,
        cast({{ process_null("BPI_FLCA") }} as string) as flca,
        cast({{ process_null("BPI_FLIDA") }} as string) as flida,
        cast({{ process_null("BPI_FLQT") }} as string) as flqt,
        cast({{ process_null("BPI_FLER") }} as string) as fler,
        cast({{ process_null("BPI_RACA") }} as string) as raca,
        cast({{ process_null("BPI_ETNIA") }} as string) as etnia,
        cast({{ process_null("BPI_NACIO") }} as string) as nacionalidade,
        cast({{ process_null("BPI_SRV") }} as string) as srv,
        cast({{ process_null("BPI_CSF") }} as string) as csf,
        cast({{ process_null("BPI_EQUIP") }} as string) as equipe,
        cast({{ process_null("BPI_EQP_AREA") }} as string) as equipe_area,
        cast({{ process_null("BPI_EQP_SEQ") }} as string) as equipe_seq,
        cast({{ process_null("BPI_CNPJ") }} as string) as cnpj,
        cast({{ process_null("trim(BPI_CEPPCN)") }} as string) as paciente_cep,
        cast({{ process_null("BPI_COD_LOGR") }} as string) as codigo_logradouro,
        cast({{ process_null("trim(BPI_LOGPCN)") }} as string) as paciente_logradouro,
        cast({{ process_null("trim(BPI_CPLPCN)") }} as string) as paciente_complemento,
        cast({{ process_null("trim(BPI_NUMPCN)") }} as string) as paciente_numero,
        cast({{ process_null("trim(BPI_BAIRRO)") }} as string) as bairro,
        cast({{ process_null("trim(BPI_DDD)") }} as string) as telefone_ddd,
        cast({{ process_null("trim(BPI_TEL)") }} as string) as telefone_numero,
        cast({{ process_null("trim(BPI_EMAIL)") }} as string) as email,
        cast({{ process_null("trim(BPI_INE)") }} as string) as ine,
        cast({{ process_null("BPI_ADVSEX") }} as string) as advsex,
        cast({{ process_null("BPI_STRUA") }} as string) as strua,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
