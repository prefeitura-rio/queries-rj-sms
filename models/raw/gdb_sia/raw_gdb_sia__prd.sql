{{
    config(
        alias="prd",
        schema= "brutos_gdb_sia",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sia_staging', 'TB_PRD') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.PRD_GESTOR") as PRD_GESTOR,
        json_extract_scalar(json, "$.PRD_CONDIC") as PRD_CONDIC,
        json_extract_scalar(json, "$.PRD_UID") as PRD_UID,
        json_extract_scalar(json, "$.PRD_CMP") as PRD_CMP,
        json_extract_scalar(json, "$.PRD_FLH") as PRD_FLH,
        json_extract_scalar(json, "$.PRD_SEQ") as PRD_SEQ,
        json_extract_scalar(json, "$.PRD_PA") as PRD_PA,
        json_extract_scalar(json, "$.PRD_CBO") as PRD_CBO,
        json_extract_scalar(json, "$.PRD_IDADE") as PRD_IDADE,
        json_extract_scalar(json, "$.PRD_QT_P") as PRD_QT_P,
        json_extract_scalar(json, "$.PRD_QT_A") as PRD_QT_A,
        json_extract_scalar(json, "$.PRD_VL_P") as PRD_VL_P,
        json_extract_scalar(json, "$.PRD_VL_A") as PRD_VL_A,
        json_extract_scalar(json, "$.PRD_MVM") as PRD_MVM,
        json_extract_scalar(json, "$.PRD_ORG") as PRD_ORG,
        json_extract_scalar(json, "$.PRD_FLPA") as PRD_FLPA,
        json_extract_scalar(json, "$.PRD_FLCBO") as PRD_FLCBO,
        json_extract_scalar(json, "$.PRD_FLCA") as PRD_FLCA,
        json_extract_scalar(json, "$.PRD_FLIDA") as PRD_FLIDA,
        json_extract_scalar(json, "$.PRD_FLQT") as PRD_FLQT,
        json_extract_scalar(json, "$.PRD_FLER") as PRD_FLER,
        json_extract_scalar(json, "$.PRD_APANUM") as PRD_APANUM,
        json_extract_scalar(json, "$.PRD_CNSMED") as PRD_CNSMED,
        json_extract_scalar(json, "$.PRD_RMS") as PRD_RMS,
        json_extract_scalar(json, "$.PRD_CNPJ") as PRD_CNPJ,
        json_extract_scalar(json, "$.PRD_NFIS") as PRD_NFIS,
        json_extract_scalar(json, "$.PRD_RESID") as PRD_RESID,
        json_extract_scalar(json, "$.PRD_RUB") as PRD_RUB,
        json_extract_scalar(json, "$.PRD_CPX") as PRD_CPX,
        json_extract_scalar(json, "$.PRD_TPFIN") as PRD_TPFIN,
        json_extract_scalar(json, "$.PRD_QTDATR") as PRD_QTDATR,
        json_extract_scalar(json, "$.PRD_QTDATU") as PRD_QTDATU,
        json_extract_scalar(json, "$.PRD_RC") as PRD_RC,
        json_extract_scalar(json, "$.PRD_CIDPRI") as PRD_CIDPRI,
        json_extract_scalar(json, "$.PRD_CIDSEC") as PRD_CIDSEC,
        json_extract_scalar(json, "$.PRD_CIDCAS") as PRD_CIDCAS,
        json_extract_scalar(json, "$.PRD_INCOUT") as PRD_INCOUT,
        json_extract_scalar(json, "$.PRD_INCURG") as PRD_INCURG,
        json_extract_scalar(json, "$.PRD_INSRG") as PRD_INSRG,
        json_extract_scalar(json, "$.PRD_CNSPCN") as PRD_CNSPCN,
        json_extract_scalar(json, "$.PRD_CPFPCT") as PRD_CPFPCT,
        json_extract_scalar(json, "$.PRD_DTINI") as PRD_DTINI,
        json_extract_scalar(json, "$.PRD_DTREA") as PRD_DTREA,
        json_extract_scalar(json, "$.PRD_SRV") as PRD_SRV,
        json_extract_scalar(json, "$.PRD_CSF") as PRD_CSF,
        json_extract_scalar(json, "$.PRD_EQUIP") as PRD_EQUIP,
        json_extract_scalar(json, "$.PRD_VL_FED") as PRD_VL_FED,
        json_extract_scalar(json, "$.PRD_VL_LOC") as PRD_VL_LOC,
        json_extract_scalar(json, "$.PRD_VL_INC") as PRD_VL_INC,
        json_extract_scalar(json, "$.PRD_RUBFED") as PRD_RUBFED,
        json_extract_scalar(json, "$.PRD_LREX") as PRD_LREX,
        json_extract_scalar(json, "$.PRD_INE") as PRD_INE,
        -- json_extract_scalar(json, "$.CHECK_SUM") as CHECK_SUM

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select
        cast({{ process_null("PRD_GESTOR") }} as string) as gestor,
        cast({{ process_null("PRD_CONDIC") }} as string) as condic,
        cast({{ process_null("PRD_UID") }} as string) as uid,
        case
            when length(trim(PRD_CMP)) = 6 and regexp_contains(trim(PRD_CMP), r'^[0-9]+$')
                then concat(left(trim(PRD_CMP), 4), '-', right(trim(PRD_CMP), 2))
            else cast({{ process_null("PRD_CMP") }} as string)
        end as mes_competencia,
        cast({{ process_null("PRD_FLH") }} as string) as flh,
        cast({{ process_null("PRD_SEQ") }} as string) as seq,
        cast({{ process_null("PRD_PA") }} as string) as pa,
        cast({{ process_null("PRD_CBO") }} as string) as cbo,
        cast({{ process_null("PRD_IDADE") }} as string) as idade,
        cast({{ process_null("PRD_QT_P") }} as int64) as quantidade_produzida,
        cast({{ process_null("PRD_QT_A") }} as int64) as quantidade_aprovada,
        cast({{ process_null("PRD_VL_P") }} as float64) as valor_produzido,  -- quanto a unidade fez
        cast({{ process_null("PRD_VL_A") }} as float64) as valor_aprovado,  -- quanto pagou de fato
        cast({{ process_null("PRD_MVM") }} as string) as mvm,
        cast({{ process_null("PRD_ORG") }} as string) as origem,
        cast({{ process_null("PRD_FLPA") }} as string) as fl_pa,
        cast({{ process_null("PRD_FLCBO") }} as string) as fl_cbo,
        cast({{ process_null("PRD_FLCA") }} as string) as fl_ca,
        cast({{ process_null("PRD_FLIDA") }} as string) as fl_ida,
        cast({{ process_null("PRD_FLQT") }} as string) as fl_qt,
        cast({{ process_null("PRD_FLER") }} as string) as fl_er,
        cast({{ process_null("PRD_APANUM") }} as string) as apa_numero,
        cast({{ process_null("PRD_CNSMED") }} as string) as medico_cns,
        cast({{ process_null("PRD_RMS") }} as string) as rms,
        cast({{ process_null("PRD_CNPJ") }} as string) as cnpj,
        cast({{ process_null("PRD_NFIS") }} as string) as nfis,
        cast({{ process_null("PRD_RESID") }} as string) as paciente_municipio_residencia,
        cast({{ process_null("trim(PRD_RUB)") }} as string) as rub,
        cast({{ process_null("PRD_CPX") }} as string) as cpx,
        cast({{ process_null("PRD_TPFIN") }} as string) as tpfin,
        cast({{ process_null("PRD_QTDATR") }} as string) as qtdatr,
        cast({{ process_null("PRD_QTDATU") }} as string) as qtdatu,
        cast({{ process_null("PRD_RC") }} as string) as rc,
        cast({{ process_null("PRD_CIDPRI") }} as string) as cid_primario,
        cast({{ process_null("PRD_CIDSEC") }} as string) as cid_secundario,
        cast({{ process_null("PRD_CIDCAS") }} as string) as cid_cas,
        cast({{ process_null("PRD_INCOUT") }} as string) as incout,
        cast({{ process_null("PRD_INCURG") }} as string) as incurg,
        cast({{ process_null("PRD_INSRG") }} as string) as insrg,
        cast({{ process_null("PRD_CNSPCN") }} as string) as paciente_cns,
        cast({{ process_null("PRD_CPFPCT") }} as string) as paciente_cpf,
        safe.parse_date("%Y%m%d", {{ process_null("PRD_DTINI") }}) as data_inicio,
        safe.parse_date("%Y%m%d", {{ process_null("PRD_DTREA") }}) as data_realizacao,
        cast({{ process_null("PRD_SRV") }} as string) as srv,
        cast({{ process_null("PRD_CSF") }} as string) as csf,
        cast({{ process_null("PRD_EQUIP") }} as string) as equipe,
        cast({{ process_null("PRD_VL_FED") }} as float64) as fed_valor, -- valor federal
        cast({{ process_null("PRD_VL_LOC") }} as float64) as loc_valor,  -- valor local
        cast({{ process_null("PRD_VL_INC") }} as float64) as inc_valor,  -- ...incremento? soma dos dois?
        cast({{ process_null("PRD_RUBFED") }} as string) as rubfed,
        cast({{ process_null("PRD_LREX") }} as string) as lrex,
        cast({{ process_null("trim(PRD_INE)") }} as string) as ine,

        -- Podem ser usados posteriormente para deduplicação
        safe_cast(data_particao as date) as data_particao,
        safe_cast(_loaded_at as timestamp) as data_carga
    from extracted
)
select *
from renamed
