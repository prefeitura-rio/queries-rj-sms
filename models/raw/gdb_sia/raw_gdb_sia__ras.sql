{{
    config(
        alias="ras",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_RAS') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.RA_GESTOR") as RA_GESTOR,
        json_extract_scalar(json, "$.RA_INSRG") as RA_INSRG,
        json_extract_scalar(json, "$.RA_UID") as RA_UID,
        json_extract_scalar(json, "$.RA_CMP") as RA_CMP,
        json_extract_scalar(json, "$.RA_CPFPCT") as RA_CPFPCT,
        json_extract_scalar(json, "$.RA_CNSPCT") as RA_CNSPCT,
        json_extract_scalar(json, "$.RA_DTINIC") as RA_DTINIC,
        json_extract_scalar(json, "$.RA_DTFIM") as RA_DTFIM,
        json_extract_scalar(json, "$.RA_NMPCN") as RA_NMPCN,
        json_extract_scalar(json, "$.RA_NPRONT") as RA_NPRONT,
        json_extract_scalar(json, "$.RA_NACPCN") as RA_NACPCN,
        json_extract_scalar(json, "$.RA_MAEPCN") as RA_MAEPCN,
        json_extract_scalar(json, "$.RA_NOMERE") as RA_NOMERE,
        json_extract_scalar(json, "$.RA_LOGPCN") as RA_LOGPCN,
        json_extract_scalar(json, "$.RA_NUMPCN") as RA_NUMPCN,
        json_extract_scalar(json, "$.RA_CPLPCN") as RA_CPLPCN,
        json_extract_scalar(json, "$.RA_CEPPCN") as RA_CEPPCN,
        json_extract_scalar(json, "$.RA_MUNPCN") as RA_MUNPCN,
        json_extract_scalar(json, "$.RA_DTNASC") as RA_DTNASC,
        json_extract_scalar(json, "$.RA_SEXPCN") as RA_SEXPCN,
        json_extract_scalar(json, "$.RA_RACA") as RA_RACA,
        json_extract_scalar(json, "$.RA_ETNIA") as RA_ETNIA,
        json_extract_scalar(json, "$.RA_TELEF") as RA_TELEF,
        json_extract_scalar(json, "$.RA_CELULAR") as RA_CELULAR,
        json_extract_scalar(json, "$.RA_MOTCOB") as RA_MOTCOB,
        json_extract_scalar(json, "$.RA_DTOBAL") as RA_DTOBAL,
        json_extract_scalar(json, "$.RA_CATEND") as RA_CATEND,
        json_extract_scalar(json, "$.RA_CIDPRI") as RA_CIDPRI,
        json_extract_scalar(json, "$.RA_CIDCA") as RA_CIDCA,
        json_extract_scalar(json, "$.RA_CIDSEC1") as RA_CIDSEC1,
        json_extract_scalar(json, "$.RA_CIDSEC2") as RA_CIDSEC2,
        json_extract_scalar(json, "$.RA_CIDSEC3") as RA_CIDSEC3,
        json_extract_scalar(json, "$.RA_PCNORI") as RA_PCNORI,
        json_extract_scalar(json, "$.RA_CODESF") as RA_CODESF,
        json_extract_scalar(json, "$.RA_CNESESF") as RA_CNESESF,
        json_extract_scalar(json, "$.RA_DESTPCT") as RA_DESTPCT,
        json_extract_scalar(json, "$.RA_ORG") as RA_ORG,
        -- json_extract_scalar(json, "$.RA_CHKSU"): as RA_CHKSU,
        json_extract_scalar(json, "$.RA_RMS") as RA_RMS,
        json_extract_scalar(json, "$.RA_DTGER") as RA_DTGER,
        json_extract_scalar(json, "$.RA_FLER") as RA_FLER,
        json_extract_scalar(json, "$.RA_INERPP") as RA_INERPP,
        json_extract_scalar(json, "$.RA_MVM") as RA_MVM,
        json_extract_scalar(json, "$.RA_STRUA") as RA_STRUA,
        json_extract_scalar(json, "$.RA_USUDRGA") as RA_USUDRGA,
        json_extract_scalar(json, "$.RA_TPDRGA") as RA_TPDRGA,
        json_extract_scalar(json, "$.RA_NAUTO") as RA_NAUTO,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("RA_GESTOR") }} as string) as gestor,
        cast({{ process_null("RA_INSRG") }} as string) as insrg,
        cast({{ process_null("RA_UID") }} as string) as uid,
        case
            when length(trim(RA_CMP)) = 6 and regexp_contains(trim(RA_CMP), r'^[0-9]+$')
                then concat(left(trim(RA_CMP), 4), '-', right(trim(RA_CMP), 2))
            else cast({{ process_null("RA_CMP") }} as string)
        end as mes_competencia,

        case
            when REGEXP_CONTAINS(trim(RA_CPFPCT), r"^0+$")
                then null
            else cast({{ process_null("RA_CPFPCT") }} as string)
        end as paciente_cpf,
        case
            when REGEXP_CONTAINS(trim(RA_CNSPCT), r"^0+$")
                then null
            else cast({{ process_null("RA_CNSPCT") }} as string)
        end as paciente_cns,
        safe.parse_date("%Y%m%d", {{ process_null("RA_DTINIC") }}) as data_inicio,
        safe.parse_date("%Y%m%d", {{ process_null("RA_DTFIM") }}) as data_fim,
        cast({{ process_null("trim(RA_NMPCN)") }} as string) as paciente_nome,
        cast({{ process_null("trim(RA_NPRONT)") }} as string) as numero_prontuario,
        cast({{ process_null("RA_NACPCN") }} as string) as paciente_nac, -- nacionalidade?
        cast({{ process_null("trim(RA_MAEPCN)") }} as string) as paciente_nome_mae,
        cast({{ process_null("trim(RA_NOMERE)") }} as string) as paciente_nome_responsavel,
        cast({{ process_null("trim(RA_LOGPCN)") }} as string) as paciente_endereco_logradouro,
        cast({{ process_null("trim(RA_NUMPCN)") }} as string) as paciente_endereco_numero,
        cast({{ process_null("trim(RA_CPLPCN)") }} as string) as paciente_endereco_complemento,
        cast({{ process_null("trim(RA_CEPPCN)") }} as string) as paciente_endereco_cep,
        cast({{ process_null("RA_MUNPCN") }} as string) as paciente_endereco_municipio,

        safe.parse_date("%Y%m%d", {{ process_null("RA_DTNASC") }}) as data_nascimento,
        case 
            when lower(trim(RA_SEXPCN)) = 'f' then 'feminino'
            when lower(trim(RA_SEXPCN)) = 'm' then 'masculino'
            else cast({{ process_null("RA_SEXPCN") }} as string)
        end as paciente_sexo,
        cast({{ process_null("RA_RACA") }} as string) as raca,
        cast({{ process_null("RA_ETNIA") }} as string) as etnia,

        case
            when REGEXP_CONTAINS(trim(RA_TELEF), r"^0+$")
                then null
            else cast({{ process_null("RA_TELEF") }} as string)
        end as telefone_numero,
        case
            when REGEXP_CONTAINS(trim(RA_CELULAR), r"^0+$")
                then null
            else cast({{ process_null("RA_CELULAR") }} as string)
        end as celular_numero,

        cast({{ process_null("RA_MOTCOB") }} as string) as motcob,
        cast({{ process_null("RA_DTOBAL") }} as string) as dtobal,
        cast({{ process_null("RA_CATEND") }} as string) as catend,
        cast({{ process_null("trim(RA_CIDPRI)") }} as string) as cid_primario,
        cast({{ process_null("trim(RA_CIDCA)") }} as string) as cid_ca,
        cast({{ process_null("trim(RA_CIDSEC1)") }} as string) as cid_secundario_1,
        cast({{ process_null("trim(RA_CIDSEC2)") }} as string) as cid_secundario_2,
        cast({{ process_null("trim(RA_CIDSEC3)") }} as string) as cid_secundario_3,
        cast({{ process_null("RA_PCNORI") }} as string) as pcnori,
        cast({{ process_null("RA_CODESF") }} as string) as codigo_esf,
        cast({{ process_null("RA_CNESESF") }} as string) as cnes_esf,
        cast({{ process_null("RA_DESTPCT") }} as string) as destpct,
        cast({{ process_null("RA_ORG") }} as string) as org,
        cast({{ process_null("RA_RMS") }} as string) as rms,
        safe.parse_date("%Y%m%d", {{ process_null("RA_DTGER") }}) as data_ger,
        cast({{ process_null("RA_FLER") }} as string) as fler,
        cast({{ process_null("RA_INERPP") }} as string) as inerpp,
        cast({{ process_null("RA_MVM") }} as string) as mvm,
        cast({{ process_null("RA_STRUA") }} as string) as strua,
        cast({{ process_null("RA_USUDRGA") }} as string) as usudrga,
        cast({{ process_null("RA_TPDRGA") }} as string) as tpdrga,
        cast({{ process_null("RA_NAUTO") }} as string) as nauto,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
