{{
    config(
        alias="aih_historico",
        schema= "brutos_gdb_sih"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_sih_staging', 'TB_HAIH') }}
),
extracted as (
    select

        json_extract_scalar(json, "$.AH_SEQ") as AH_SEQ,
        json_extract_scalar(json, "$.AH_SITUACAO") as AH_SITUACAO,
        json_extract_scalar(json, "$.AH_LOTE") as AH_LOTE,
        json_extract_scalar(json, "$.AH_LOTE_APRES") as AH_LOTE_APRES,
        json_extract_scalar(json, "$.AH_IDENT") as AH_IDENT,
        json_extract_scalar(json, "$.AH_ESPECIALIDADE") as AH_ESPECIALIDADE,
        json_extract_scalar(json, "$.AH_NUM_AIH") as AH_NUM_AIH,
        json_extract_scalar(json, "$.AH_NUM_AIH_PROX") as AH_NUM_AIH_PROX,
        json_extract_scalar(json, "$.AH_NUM_AIH_ANT") as AH_NUM_AIH_ANT,
        json_extract_scalar(json, "$.AH_SEQ_AIH5") as AH_SEQ_AIH5,
        json_extract_scalar(json, "$.AH_CMPT") as AH_CMPT,
        json_extract_scalar(json, "$.AH_OE_AIH") as AH_OE_AIH,
        json_extract_scalar(json, "$.AH_OE_GESTOR") as AH_OE_GESTOR,
        json_extract_scalar(json, "$.AH_OE_REGIONAL") as AH_OE_REGIONAL,
        json_extract_scalar(json, "$.AH_CNES") as AH_CNES,
        json_extract_scalar(json, "$.AH_MUN_HOSP") as AH_MUN_HOSP,
        json_extract_scalar(json, "$.AH_DT_EMISSAO") as AH_DT_EMISSAO,
        json_extract_scalar(json, "$.AH_DT_INTERNACAO") as AH_DT_INTERNACAO,
        json_extract_scalar(json, "$.AH_DT_SAIDA") as AH_DT_SAIDA,
        json_extract_scalar(json, "$.AH_PROC_SOLICITADO") as AH_PROC_SOLICITADO,
        json_extract_scalar(json, "$.AH_ST_MUDA_PROC") as AH_ST_MUDA_PROC,
        json_extract_scalar(json, "$.AH_PROC_REALIZADO") as AH_PROC_REALIZADO,
        json_extract_scalar(json, "$.AH_CAR_INTERNACAO") as AH_CAR_INTERNACAO,
        json_extract_scalar(json, "$.AH_MODALIDADE_INTERNACAO") as AH_MODALIDADE_INTERNACAO,
        json_extract_scalar(json, "$.AH_MOT_SAIDA") as AH_MOT_SAIDA,
        json_extract_scalar(json, "$.AH_MED_SOL_IDENT") as AH_MED_SOL_IDENT,
        json_extract_scalar(json, "$.AH_MED_SOL_DOC") as AH_MED_SOL_DOC,
        json_extract_scalar(json, "$.AH_MED_RESP_IDENT") as AH_MED_RESP_IDENT,
        json_extract_scalar(json, "$.AH_MED_RESP_DOC") as AH_MED_RESP_DOC,
        json_extract_scalar(json, "$.AH_DIR_CLINICO_IDENT") as AH_DIR_CLINICO_IDENT,
        json_extract_scalar(json, "$.AH_DIR_CLINICO_DOC") as AH_DIR_CLINICO_DOC,
        json_extract_scalar(json, "$.AH_AUTORIZADOR_IDENT") as AH_AUTORIZADOR_IDENT,
        json_extract_scalar(json, "$.AH_AUTORIZADOR_DOC") as AH_AUTORIZADOR_DOC,
        json_extract_scalar(json, "$.AH_DIAG_PRI") as AH_DIAG_PRI,
        json_extract_scalar(json, "$.AH_DIAG_SEC") as AH_DIAG_SEC,
        json_extract_scalar(json, "$.AH_DIAG_COMP") as AH_DIAG_COMP,
        json_extract_scalar(json, "$.AH_DIAG_OBITO") as AH_DIAG_OBITO,
        json_extract_scalar(json, "$.AH_PACIENTE_NOME") as AH_PACIENTE_NOME,
        json_extract_scalar(json, "$.AH_PACIENTE_DT_NASCIMENTO") as AH_PACIENTE_DT_NASCIMENTO,
        json_extract_scalar(json, "$.AH_PACIENTE_SEXO") as AH_PACIENTE_SEXO,
        json_extract_scalar(json, "$.AH_PACIENTE_RACA_COR") as AH_PACIENTE_RACA_COR,
        json_extract_scalar(json, "$.AH_PACIENTE_NOME_RESP") as AH_PACIENTE_NOME_RESP,
        json_extract_scalar(json, "$.AH_PACIENTE_NOME_MAE") as AH_PACIENTE_NOME_MAE,
        json_extract_scalar(json, "$.AH_PACIENTE_IDENT_DOC") as AH_PACIENTE_IDENT_DOC,
        json_extract_scalar(json, "$.AH_PACIENTE_NUMERO_DOC") as AH_PACIENTE_NUMERO_DOC,
        json_extract_scalar(json, "$.AH_PACIENTE_NUMERO_CNS") as AH_PACIENTE_NUMERO_CNS,
        json_extract_scalar(json, "$.AH_PACIENTE_NACIONALIDADE") as AH_PACIENTE_NACIONALIDADE,
        json_extract_scalar(json, "$.AH_PACIENTE_MUN_ORIGEM") as AH_PACIENTE_MUN_ORIGEM,
        json_extract_scalar(json, "$.AH_PACIENTE_TIPO_LOGR") as AH_PACIENTE_TIPO_LOGR,
        json_extract_scalar(json, "$.AH_PACIENTE_LOGR") as AH_PACIENTE_LOGR,
        json_extract_scalar(json, "$.AH_PACIENTE_LOGR_NUMERO") as AH_PACIENTE_LOGR_NUMERO,
        json_extract_scalar(json, "$.AH_PACIENTE_LOGR_COMPL") as AH_PACIENTE_LOGR_COMPL,
        json_extract_scalar(json, "$.AH_PACIENTE_LOGR_BAIRRO") as AH_PACIENTE_LOGR_BAIRRO,
        json_extract_scalar(json, "$.AH_PACIENTE_LOGR_MUNICIPIO") as AH_PACIENTE_LOGR_MUNICIPIO,
        json_extract_scalar(json, "$.AH_PACIENTE_LOGR_UF") as AH_PACIENTE_LOGR_UF,
        json_extract_scalar(json, "$.AH_PACIENTE_LOGR_CEP") as AH_PACIENTE_LOGR_CEP,
        json_extract_scalar(json, "$.AH_PACIENTE_IDADE") as AH_PACIENTE_IDADE,
        json_extract_scalar(json, "$.AH_PRONTUARIO") as AH_PRONTUARIO,
        json_extract_scalar(json, "$.AH_ENFERMARIA") as AH_ENFERMARIA,
        json_extract_scalar(json, "$.AH_LEITO") as AH_LEITO,
        json_extract_scalar(json, "$.AH_UTINEO_MOT_SAIDA") as AH_UTINEO_MOT_SAIDA,
        json_extract_scalar(json, "$.AH_UTINEO_PESO") as AH_UTINEO_PESO,
        json_extract_scalar(json, "$.AH_UTINEO_MESES_GESTACAO") as AH_UTINEO_MESES_GESTACAO,
        json_extract_scalar(json, "$.AH_ACDTRAB_CNPJ_EMP") as AH_ACDTRAB_CNPJ_EMP,
        json_extract_scalar(json, "$.AH_ACDTRAB_CBOR") as AH_ACDTRAB_CBOR,
        json_extract_scalar(json, "$.AH_ACDTRAB_CNAER") as AH_ACDTRAB_CNAER,
        json_extract_scalar(json, "$.AH_ACDTRAB_VINC_PREV") as AH_ACDTRAB_VINC_PREV,
        json_extract_scalar(json, "$.AH_PARTO_QTD_NASC_VIVOS") as AH_PARTO_QTD_NASC_VIVOS,
        json_extract_scalar(json, "$.AH_PARTO_QTD_NASC_MORTOS") as AH_PARTO_QTD_NASC_MORTOS,
        json_extract_scalar(json, "$.AH_PARTO_QTD_ALTA") as AH_PARTO_QTD_ALTA,
        json_extract_scalar(json, "$.AH_PARTO_QTD_TRAN") as AH_PARTO_QTD_TRAN,
        json_extract_scalar(json, "$.AH_PARTO_QTD_OBITO") as AH_PARTO_QTD_OBITO,
        json_extract_scalar(json, "$.AH_PARTO_NUM_PRENATAL") as AH_PARTO_NUM_PRENATAL,
        json_extract_scalar(json, "$.AH_LAQVAS_QTD_FILHOS") as AH_LAQVAS_QTD_FILHOS,
        json_extract_scalar(json, "$.AH_LAQVAS_GRAU_INSTRUC") as AH_LAQVAS_GRAU_INSTRUC,
        json_extract_scalar(json, "$.AH_LAQVAS_CID_INDICACAO") as AH_LAQVAS_CID_INDICACAO,
        json_extract_scalar(json, "$.AH_LAQVAS_MET_CONTRACEP1") as AH_LAQVAS_MET_CONTRACEP1,
        json_extract_scalar(json, "$.AH_LAQVAS_MET_CONTRACEP2") as AH_LAQVAS_MET_CONTRACEP2,
        json_extract_scalar(json, "$.AH_LAQVAS_GESTACAO_RISCO") as AH_LAQVAS_GESTACAO_RISCO,
        json_extract_scalar(json, "$.AH_VERSAO_SISAIH01") as AH_VERSAO_SISAIH01,
        json_extract_scalar(json, "$.AH_ST_DUPLICIDADE") as AH_ST_DUPLICIDADE,
        json_extract_scalar(json, "$.AH_ST_BLOQUEIO") as AH_ST_BLOQUEIO,
        json_extract_scalar(json, "$.AH_ST_AGRAVO") as AH_ST_AGRAVO,
        json_extract_scalar(json, "$.AH_MOT_BLOQ") as AH_MOT_BLOQ,
        json_extract_scalar(json, "$.AH_IN_GER_INF") as AH_IN_GER_INF,
        json_extract_scalar(json, "$.AH_GESTOR_IDENT") as AH_GESTOR_IDENT,
        json_extract_scalar(json, "$.AH_GESTOR_DOC") as AH_GESTOR_DOC,
        json_extract_scalar(json, "$.AH_COD_SOL_LIB") as AH_COD_SOL_LIB,
        json_extract_scalar(json, "$.AH_ST_INTO") as AH_ST_INTO,
        json_extract_scalar(json, "$.AH_CONTRATO") as AH_CONTRATO,
        json_extract_scalar(json, "$.AH_IVD_SH") as AH_IVD_SH,
        json_extract_scalar(json, "$.AH_IVD_SP") as AH_IVD_SP,
        json_extract_scalar(json, "$.AH_DIARIAS") as AH_DIARIAS,
        json_extract_scalar(json, "$.AH_DIARIAS_UTI") as AH_DIARIAS_UTI,
        json_extract_scalar(json, "$.AH_DIARIAS_UI") as AH_DIARIAS_UI,
        json_extract_scalar(json, "$.AH_ST_CATETERISMO_ANEST") as AH_ST_CATETERISMO_ANEST,
        json_extract_scalar(json, "$.AH_COMPLEXIDADE") as AH_COMPLEXIDADE,
        json_extract_scalar(json, "$.AH_FINANCIAMENTO") as AH_FINANCIAMENTO,
        json_extract_scalar(json, "$.AH_TIPO_FAEC") as AH_TIPO_FAEC,
        json_extract_scalar(json, "$.AH_CS") as AH_CS,
        json_extract_scalar(json, "$.AH_STATUS_PR") as AH_STATUS_PR,
        json_extract_scalar(json, "$.AH_CRC") as AH_CRC,
        json_extract_scalar(json, "$.AH_PACIENTE_ETNIA") as AH_PACIENTE_ETNIA,
        json_extract_scalar(json, "$.AH_PACIENTE_TEL_DDD") as AH_PACIENTE_TEL_DDD,
        json_extract_scalar(json, "$.AH_PACIENTE_TEL_NUM") as AH_PACIENTE_TEL_NUM,
        json_extract_scalar(json, "$.AH_AUDIT_JUST") as AH_AUDIT_JUST,
        json_extract_scalar(json, "$.AH_AUDIT_SISAIH01_JUST") as AH_AUDIT_SISAIH01_JUST,
        json_extract_scalar(json, "$.AH_STATUS_PR1") as AH_STATUS_PR1,
        json_extract_scalar(json, "$.AH_ST_MENTAL") as AH_ST_MENTAL,
        json_extract_scalar(json, "$.AH_ST_DUPLICIDADE_CNS") as AH_ST_DUPLICIDADE_CNS,
        json_extract_scalar(json, "$.AH_PACIENTE_FONETICO_NOME") as AH_PACIENTE_FONETICO_NOME,
        json_extract_scalar(json, "$.AH_PACIENTE_FONETICO_NOME_MAE") as AH_PACIENTE_FONETICO_NOME_MAE,
        json_extract_scalar(json, "$.AH_ST_ORTO") as AH_ST_ORTO,
        json_extract_scalar(json, "$.AH_ST_NEURO") as AH_ST_NEURO,
        json_extract_scalar(json, "$.AH_ST_ONCO") as AH_ST_ONCO,
        json_extract_scalar(json, "$.AH_DIAG_SEC_1") as AH_DIAG_SEC_1,
        json_extract_scalar(json, "$.AH_DIAG_SEC_1_CLASS") as AH_DIAG_SEC_1_CLASS,
        json_extract_scalar(json, "$.AH_DIAG_SEC_2") as AH_DIAG_SEC_2,
        json_extract_scalar(json, "$.AH_DIAG_SEC_2_CLASS") as AH_DIAG_SEC_2_CLASS,
        json_extract_scalar(json, "$.AH_DIAG_SEC_3") as AH_DIAG_SEC_3,
        json_extract_scalar(json, "$.AH_DIAG_SEC_3_CLASS") as AH_DIAG_SEC_3_CLASS,
        json_extract_scalar(json, "$.AH_DIAG_SEC_4") as AH_DIAG_SEC_4,
        json_extract_scalar(json, "$.AH_DIAG_SEC_4_CLASS") as AH_DIAG_SEC_4_CLASS,
        json_extract_scalar(json, "$.AH_DIAG_SEC_5") as AH_DIAG_SEC_5,
        json_extract_scalar(json, "$.AH_DIAG_SEC_5_CLASS") as AH_DIAG_SEC_5_CLASS,
        json_extract_scalar(json, "$.AH_DIAG_SEC_6") as AH_DIAG_SEC_6,
        json_extract_scalar(json, "$.AH_DIAG_SEC_6_CLASS") as AH_DIAG_SEC_6_CLASS,
        json_extract_scalar(json, "$.AH_DIAG_SEC_7") as AH_DIAG_SEC_7,
        json_extract_scalar(json, "$.AH_DIAG_SEC_7_CLASS") as AH_DIAG_SEC_7_CLASS,
        json_extract_scalar(json, "$.AH_DIAG_SEC_8") as AH_DIAG_SEC_8,
        json_extract_scalar(json, "$.AH_DIAG_SEC_8_CLASS") as AH_DIAG_SEC_8_CLASS,
        json_extract_scalar(json, "$.AH_DIAG_SEC_9") as AH_DIAG_SEC_9,
        json_extract_scalar(json, "$.AH_DIAG_SEC_9_CLASS") as AH_DIAG_SEC_9_CLASS,
        json_extract_scalar(json, "$.AH_PACIENTE_DADOS_VALIDADOS_CNS") as AH_PACIENTE_DADOS_VALIDADOS_CNS,
        json_extract_scalar(json, "$.AH_ST_INTERNACAO_CONCOM") as AH_ST_INTERNACAO_CONCOM,
        json_extract_scalar(json, "$.AH_ST_INTERNACAO_CONCOM_BDNAIH") as AH_ST_INTERNACAO_CONCOM_BDNAIH,
        json_extract_scalar(json, "$.AH_STATUS_PR2") as AH_STATUS_PR2,
        json_extract_scalar(json, "$.AH_STATUS_PR3") as AH_STATUS_PR3,
        json_extract_scalar(json, "$.AH_STATUS_PR4") as AH_STATUS_PR4,
        json_extract_scalar(json, "$.AH_STATUS_PR5") as AH_STATUS_PR5,
        json_extract_scalar(json, "$.AH_STATUS_PR6") as AH_STATUS_PR6,
        json_extract_scalar(json, "$.AH_STATUS_PR7") as AH_STATUS_PR7,
        json_extract_scalar(json, "$.AH_PACIENTE_NUMERO_CPF") as AH_PACIENTE_NUMERO_CPF,
        json_extract_scalar(json, "$.AH_ST_DUPLICIDADE_CPF") as AH_ST_DUPLICIDADE_CPF,
        json_extract_scalar(json, "$.AH_PACIENTE_SIT_RUA") as AH_PACIENTE_SIT_RUA,

        _source_file,
        _loaded_at,
        data_particao
    from source
),

renamed as (
    select

        cast({{ process_null("AH_SEQ") }} as string) as seq,
        cast({{ process_null("AH_SITUACAO") }} as string) as situacao,
        cast({{ process_null("AH_LOTE") }} as string) as lote,
        cast({{ process_null("AH_LOTE_APRES") }} as string) as lote_apres,
        cast({{ process_null("AH_IDENT") }} as string) as ident,
        cast({{ process_null("AH_ESPECIALIDADE") }} as string) as especialidade,
        cast({{ process_null("AH_NUM_AIH") }} as string) as numero_aih,
        case
            when REGEXP_CONTAINS(trim(AH_NUM_AIH_ANT), r"^0+$")
                then null
            else cast({{ process_null("AH_NUM_AIH_ANT") }} as string)
        end as numero_aih_anterior,
        case
            when REGEXP_CONTAINS(trim(AH_NUM_AIH_PROX), r"^0+$")
                then null
            else cast({{ process_null("AH_NUM_AIH_PROX") }} as string)
        end as numero_aih_proximo,
        cast({{ process_null("AH_SEQ_AIH5") }} as string) as numero_seq_aih5,
        cast({{ process_null("AH_CMPT") }} as string) as cmpt,
        cast({{ process_null("AH_OE_AIH") }} as string) as oe_aih,
        cast({{ process_null("AH_OE_GESTOR") }} as string) as oe_gestor,
        cast({{ process_null("AH_OE_REGIONAL") }} as string) as oe_regional,
        cast({{ process_null("AH_CNES") }} as string) as id_cnes,
        cast({{ process_null("AH_MUN_HOSP") }} as string) as municipio_hospital,
        cast({{ process_null("AH_DT_EMISSAO") }} as string) as data_emissao,
        cast({{ process_null("AH_DT_INTERNACAO") }} as string) as data_internacao,
        cast({{ process_null("AH_DT_SAIDA") }} as string) as data_saida,
        cast({{ process_null("AH_PROC_SOLICITADO") }} as string) as procedimento_solicitado,
        cast({{ process_null("AH_PROC_REALIZADO") }} as string) as procedimento_realizado,
        cast({{ process_null("AH_CAR_INTERNACAO") }} as string) as car_internacao,
        cast({{ process_null("AH_MODALIDADE_INTERNACAO") }} as string) as modalidade_internacao,
        cast({{ process_null("AH_MOT_SAIDA") }} as string) as motivo_saida,
        cast({{ process_null("AH_MED_SOL_IDENT") }} as string) as medico_solicitante_ident,
        cast({{ process_null("AH_MED_SOL_DOC") }} as string) as medico_solicitante_doc,
        cast({{ process_null("AH_MED_RESP_IDENT") }} as string) as medico_responsavel_ident,
        cast({{ process_null("AH_MED_RESP_DOC") }} as string) as medico_responsavel_doc,
        cast({{ process_null("AH_DIR_CLINICO_IDENT") }} as string) as dir_clinico_ident,
        cast({{ process_null("AH_DIR_CLINICO_DOC") }} as string) as dir_clinico_doc,
        cast({{ process_null("AH_AUTORIZADOR_IDENT") }} as string) as autorizador_ident,
        cast({{ process_null("AH_AUTORIZADOR_DOC") }} as string) as autorizador_doc,

        -- Paciente
        cast(trim({{ process_null("AH_PACIENTE_NOME") }}) as string) as paciente_nome,
        cast({{ process_null("AH_PACIENTE_DT_NASCIMENTO") }} as string) as paciente_data_nascimento,
        cast({{ process_null("AH_PACIENTE_IDADE") }} as string) as paciente_idade,
        cast({{ process_null("AH_PACIENTE_SEXO") }} as string) as paciente_sexo,
        cast({{ process_null("AH_PACIENTE_RACA_COR") }} as string) as paciente_raca,
        cast(trim({{ process_null("AH_PACIENTE_NOME_RESP") }}) as string) as paciente_nome_responsavel,
        cast(trim({{ process_null("AH_PACIENTE_NOME_MAE") }}) as string) as paciente_nome_mae,
        cast({{ process_null("AH_PACIENTE_IDENT_DOC") }} as string) as paciente_ident_doc,
        cast({{ process_null("AH_PACIENTE_NUMERO_DOC") }} as string) as paciente_numero_doc,
        cast({{ process_null("AH_PACIENTE_ETNIA") }} as string) as paciente_etnia,
        cast({{ process_null("AH_PACIENTE_TEL_DDD") }} as string) as paciente_tel_ddd,
        cast({{ process_null("AH_PACIENTE_TEL_NUM") }} as string) as paciente_tel_numero,
        case
            when REGEXP_CONTAINS(trim(AH_PACIENTE_NUMERO_CPF), r"^0+$")
                then null
            else cast({{ process_null("AH_PACIENTE_NUMERO_CPF") }} as string)
        end as paciente_numero_cpf,
        case
            when REGEXP_CONTAINS(trim(AH_PACIENTE_NUMERO_CNS), r"^0+$")
                then null
            else cast({{ process_null("AH_PACIENTE_NUMERO_CNS") }} as string)
        end as paciente_numero_cns,
        cast({{ process_null("AH_PACIENTE_SIT_RUA") }} as string) as paciente_situacao_rua,
        cast({{ process_null("AH_PACIENTE_NACIONALIDADE") }} as string) as paciente_nacionalidade,
        cast({{ process_null("AH_PACIENTE_MUN_ORIGEM") }} as string) as paciente_municipio_origem,
        cast({{ process_null("AH_PACIENTE_TIPO_LOGR") }} as string) as paciente_tipo_logradouro,
        cast(trim({{ process_null("AH_PACIENTE_LOGR") }}) as string) as paciente_logradouro,
        cast({{ process_null("AH_PACIENTE_LOGR_NUMERO") }} as string) as paciente_logradouro_numero,
        cast({{ process_null("AH_PACIENTE_LOGR_COMPL") }} as string) as paciente_logradouro_complemento,
        cast(trim({{ process_null("AH_PACIENTE_LOGR_BAIRRO") }}) as string) as paciente_logradouro_bairro,
        cast({{ process_null("AH_PACIENTE_LOGR_MUNICIPIO") }} as string) as paciente_logradouro_municipio,
        cast({{ process_null("AH_PACIENTE_LOGR_UF") }} as string) as paciente_logradouro_uf,
        cast({{ process_null("AH_PACIENTE_LOGR_CEP") }} as string) as paciente_logradouro_cep,
        cast({{ process_null("AH_PRONTUARIO") }} as string) as prontuario,
        cast({{ process_null("AH_ENFERMARIA") }} as string) as enfermaria,
        cast({{ process_null("AH_LEITO") }} as string) as leito,

        -- CIDs
        case
            when REGEXP_CONTAINS(trim(AH_DIAG_PRI), r"^0+$")
                then null
            else cast({{ process_null("AH_DIAG_PRI") }} as string)
        end as cid_diagnostico_principal,
        case
            when REGEXP_CONTAINS(trim(AH_DIAG_SEC), r"^0+$")
                then null
            else cast({{ process_null("AH_DIAG_SEC") }} as string)
        end as cid_diagnostico_secundario,
        case
            when REGEXP_CONTAINS(trim(AH_DIAG_COMP), r"^0+$")
                then null
            else cast({{ process_null("AH_DIAG_COMP") }} as string)
        end as cid_diagnostico_comp, -- complementar?
        case
            when REGEXP_CONTAINS(trim(AH_DIAG_OBITO), r"^0+$")
                then null
            else cast({{ process_null("AH_DIAG_OBITO") }} as string)
        end as cid_diagnostico_obito,
        cast({{ process_null("AH_DIAG_SEC_1") }} as string) as cid_diagnostico_secundario_1,
        cast({{ process_null("AH_DIAG_SEC_1_CLASS") }} as string) as cid_diag_sec_1_class,
        cast({{ process_null("AH_DIAG_SEC_2") }} as string) as cid_diagnostico_secundario_2,
        cast({{ process_null("AH_DIAG_SEC_2_CLASS") }} as string) as cid_diag_sec_2_class,
        cast({{ process_null("AH_DIAG_SEC_3") }} as string) as cid_diagnostico_secundario_3,
        cast({{ process_null("AH_DIAG_SEC_3_CLASS") }} as string) as cid_diag_sec_3_class,
        cast({{ process_null("AH_DIAG_SEC_4") }} as string) as cid_diagnostico_secundario_4,
        cast({{ process_null("AH_DIAG_SEC_4_CLASS") }} as string) as cid_diag_sec_4_class,
        cast({{ process_null("AH_DIAG_SEC_5") }} as string) as cid_diagnostico_secundario_5,
        cast({{ process_null("AH_DIAG_SEC_5_CLASS") }} as string) as cid_diag_sec_5_class,
        cast({{ process_null("AH_DIAG_SEC_6") }} as string) as cid_diagnostico_secundario_6,
        cast({{ process_null("AH_DIAG_SEC_6_CLASS") }} as string) as cid_diag_sec_6_class,
        cast({{ process_null("AH_DIAG_SEC_7") }} as string) as cid_diagnostico_secundario_7,
        cast({{ process_null("AH_DIAG_SEC_7_CLASS") }} as string) as cid_diag_sec_7_class,
        cast({{ process_null("AH_DIAG_SEC_8") }} as string) as cid_diagnostico_secundario_8,
        cast({{ process_null("AH_DIAG_SEC_8_CLASS") }} as string) as cid_diag_sec_8_class,
        cast({{ process_null("AH_DIAG_SEC_9") }} as string) as cid_diagnostico_secundario_9,
        cast({{ process_null("AH_DIAG_SEC_9_CLASS") }} as string) as cid_diag_sec_9_class,

        -- Parto
        cast({{ process_null("AH_PARTO_QTD_NASC_VIVOS") }} as string) as parto_quantidade_nascidos_vivos,
        cast({{ process_null("AH_PARTO_QTD_NASC_MORTOS") }} as string) as partio_quantidade_nascidos_mortos,
        cast({{ process_null("AH_PARTO_QTD_ALTA") }} as string) as parto_quantidade_alta,
        cast({{ process_null("AH_PARTO_QTD_TRAN") }} as string) as parto_quantidade_tran,
        cast({{ process_null("AH_PARTO_QTD_OBITO") }} as string) as parto_quantidade_obito,
        cast({{ process_null("AH_PARTO_NUM_PRENATAL") }} as string) as parto_numero_prenatal,

        -- UTINEO
        cast({{ process_null("AH_UTINEO_MOT_SAIDA") }} as string) as utineo_motivo_saida,
        cast({{ process_null("AH_UTINEO_PESO") }} as string) as utineo_peso,
        cast({{ process_null("AH_UTINEO_MESES_GESTACAO") }} as string) as utineo_meses_gestacao,

        -- ACDTRAB
        case
            when REGEXP_CONTAINS(trim(AH_ACDTRAB_CNPJ_EMP), r"^0+$")
                then null
            else cast({{ process_null("AH_ACDTRAB_CNPJ_EMP") }} as string)
        end as acdtrab_cnpj_emp,
        cast({{ process_null("AH_ACDTRAB_CBOR") }} as string) as acdtrab_cbor,
        cast({{ process_null("AH_ACDTRAB_CNAER") }} as string) as acdtrab_cnaer,
        cast({{ process_null("AH_ACDTRAB_VINC_PREV") }} as string) as acdtrab_vinc_prev,

        -- LAQVAS
        cast({{ process_null("AH_LAQVAS_QTD_FILHOS") }} as string) as laqvas_quantidade_filhos,
        cast({{ process_null("AH_LAQVAS_GRAU_INSTRUC") }} as string) as laqvas_grau_instrucao,
        cast({{ process_null("AH_LAQVAS_CID_INDICACAO") }} as string) as laqvas_cid_indicacao,
        cast({{ process_null("AH_LAQVAS_MET_CONTRACEP1") }} as string) as laqvas_metodo_contraceptivo_1,
        cast({{ process_null("AH_LAQVAS_MET_CONTRACEP2") }} as string) as laqvas_metodo_contraceptivo_2,
        cast({{ process_null("AH_LAQVAS_GESTACAO_RISCO") }} as string) as laqvas_gestacao_risco,

        -- ST?
        cast({{ process_null("AH_ST_MUDA_PROC") }} as string) as st_muda_proc,
        cast({{ process_null("AH_ST_DUPLICIDADE") }} as string) as st_duplicidade,
        cast({{ process_null("AH_ST_DUPLICIDADE_CPF") }} as string) as st_duplicidade_cpf,
        cast({{ process_null("AH_ST_DUPLICIDADE_CNS") }} as string) as st_duplicidade_cns,
        cast({{ process_null("AH_ST_BLOQUEIO") }} as string) as st_bloqueio,
        cast({{ process_null("AH_ST_AGRAVO") }} as string) as st_agravo,
        cast({{ process_null("AH_ST_INTO") }} as string) as st_into,
        cast({{ process_null("AH_ST_CATETERISMO_ANEST") }} as string) as st_cateterismo_anest,
        cast({{ process_null("AH_ST_MENTAL") }} as string) as st_mental,
        cast({{ process_null("AH_ST_ORTO") }} as string) as st_orto,
        cast({{ process_null("AH_ST_NEURO") }} as string) as st_neuro,
        cast({{ process_null("AH_ST_ONCO") }} as string) as st_onco,
        cast({{ process_null("AH_ST_INTERNACAO_CONCOM") }} as string) as st_internacao_concom,
        cast({{ process_null("AH_ST_INTERNACAO_CONCOM_BDNAIH") }} as string) as st_internacao_concom_bdnaih,
        -- ST = status?
        cast({{ process_null("AH_STATUS_PR") }} as string) as status_pr,
        cast({{ process_null("AH_STATUS_PR1") }} as string) as status_pr1,
        cast({{ process_null("AH_STATUS_PR2") }} as string) as status_pr2,
        cast({{ process_null("AH_STATUS_PR3") }} as string) as status_pr3,
        cast({{ process_null("AH_STATUS_PR4") }} as string) as status_pr4,
        cast({{ process_null("AH_STATUS_PR5") }} as string) as status_pr5,
        cast({{ process_null("AH_STATUS_PR6") }} as string) as status_pr6,
        cast({{ process_null("AH_STATUS_PR7") }} as string) as status_pr7,

        cast({{ process_null("AH_DIARIAS") }} as string) as diarias,
        cast({{ process_null("AH_DIARIAS_UTI") }} as string) as diarias_uti,
        cast({{ process_null("AH_DIARIAS_UI") }} as string) as diarias_ui,
        cast({{ process_null("AH_GESTOR_IDENT") }} as string) as gestor_ident,
        cast({{ process_null("AH_GESTOR_DOC") }} as string) as gestor_doc,

        -- ?
        cast({{ process_null("AH_MOT_BLOQ") }} as string) as motivo_bloqueio,
        cast({{ process_null("AH_IN_GER_INF") }} as string) as in_ger_inf,
        cast({{ process_null("AH_COD_SOL_LIB") }} as string) as codigo_sol_lib,
        cast({{ process_null("AH_CONTRATO") }} as string) as contrato,
        cast({{ process_null("AH_IVD_SH") }} as string) as ivd_sh,
        cast({{ process_null("AH_IVD_SP") }} as string) as ivd_sp,
        cast({{ process_null("AH_COMPLEXIDADE") }} as string) as complexidade,
        cast({{ process_null("AH_FINANCIAMENTO") }} as string) as financiamento,
        cast({{ process_null("AH_TIPO_FAEC") }} as string) as tipo_faec,
        cast({{ process_null("AH_VERSAO_SISAIH01") }} as string) as versao_sisaih01,
        cast({{ process_null("AH_AUDIT_JUST") }} as string) as audit_just,
        cast({{ process_null("AH_AUDIT_SISAIH01_JUST") }} as string) as audit_sisaih01_just,
        cast({{ process_null("AH_PACIENTE_DADOS_VALIDADOS_CNS") }} as string) as paciente_dados_vaidados_cns,

        -- Podem ser usados posteriormente para deduplicação
        data_particao,
        _loaded_at as data_carga,
    from extracted
)
select *
from renamed
