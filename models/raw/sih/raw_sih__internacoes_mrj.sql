{{
    config(
        alias="internacoes_mrj",
        materialized="table",
    )
}}


with
    sih_2008_2024 as (
        select
            *, 
            null as AH_PACIENTE_TEL_DDD,
            null as AH_PACIENTE_TEL_NUM,
            null as AH_AUDIT_JUST,
            null as AH_AUDIT_SISAIH01_JUST,
            null as AH_STATUS_PR1,
            null as AH_ST_MENTAL,
            null as AH_ST_DUPLICIDADE_CNS,
            null as AH_PACIENTE_FONETICO_NOME,
            null as AH_PACIENTE_FONETICO_NOME_MAE,
            null as AH_ST_ORTO,
            null as AH_ST_NEURO,
            null as AH_ST_ONCO,
            null as AH_DIAG_SEC_1,
            null as AH_DIAG_SEC_1_CLASS,
            null as AH_DIAG_SEC_2,
            null as AH_DIAG_SEC_2_CLASS,
            null as AH_DIAG_SEC_3,
            null as AH_DIAG_SEC_3_CLASS,
            null as AH_DIAG_SEC_4,
            null as AH_DIAG_SEC_4_CLASS,
            null as AH_DIAG_SEC_5,
            null as AH_DIAG_SEC_5_CLASS,
            null as AH_DIAG_SEC_6,
            null as AH_DIAG_SEC_6_CLASS,
            null as AH_DIAG_SEC_7,
            null as AH_DIAG_SEC_7_CLASS,
            null as AH_DIAG_SEC_8,
            null as AH_DIAG_SEC_8_CLASS,
            null as AH_DIAG_SEC_9,
            null as AH_DIAG_SEC_9_CLASS,
            null as AH_PACIENTE_DADOS_VALIDADOS_CNS,
            null as AH_ST_INTERNACAO_CONCOM,
            null as AH_ST_INTERNACAO_CONCOM_BDNAIH,
            null as AH_STATUS_PR2,
            null as AH_STATUS_PR3,
            null as AH_STATUS_PR4,
            null as AH_STATUS_PR5,
            null as AH_STATUS_PR6,
            null as AH_STATUS_PR7,
            null as AH_PACIENTE_NUMERO_CPF,
            null as AH_ST_DUPLICIDADE_CPF,
        from {{ source("brutos_sih_staging", "sih_2008_2011") }}

        union all
        -- SIH 2012
        select 
            *,
            null as AH_ST_DUPLICIDADE_CNS,
            null as AH_PACIENTE_FONETICO_NOME,
            null as AH_PACIENTE_FONETICO_NOME_MAE,
            null as AH_ST_ORTO,
            null as AH_ST_NEURO,
            null as AH_ST_ONCO,
            null as AH_DIAG_SEC_1,
            null as AH_DIAG_SEC_1_CLASS,
            null as AH_DIAG_SEC_2,
            null as AH_DIAG_SEC_2_CLASS,
            null as AH_DIAG_SEC_3,
            null as AH_DIAG_SEC_3_CLASS,
            null as AH_DIAG_SEC_4,
            null as AH_DIAG_SEC_4_CLASS,
            null as AH_DIAG_SEC_5,
            null as AH_DIAG_SEC_5_CLASS,
            null as AH_DIAG_SEC_6,
            null as AH_DIAG_SEC_6_CLASS,
            null as AH_DIAG_SEC_7,
            null as AH_DIAG_SEC_7_CLASS,
            null as AH_DIAG_SEC_8,
            null as AH_DIAG_SEC_8_CLASS,
            null as AH_DIAG_SEC_9,
            null as AH_DIAG_SEC_9_CLASS,
            null as AH_PACIENTE_DADOS_VALIDADOS_CNS,
            null as AH_ST_INTERNACAO_CONCOM,
            null as AH_ST_INTERNACAO_CONCOM_BDNAIH,
            null as AH_STATUS_PR2,
            null as AH_STATUS_PR3,
            null as AH_STATUS_PR4,
            null as AH_STATUS_PR5,
            null as AH_STATUS_PR6,
            null as AH_STATUS_PR7,
            null as AH_PACIENTE_NUMERO_CPF,
            null as AH_ST_DUPLICIDADE_CPF
        from {{ source("brutos_sih_staging", "sih_2012") }}

        union all
        -- SIH 2012 2014
        select 
            *,
            null as AH_DIAG_SEC_1,
            null as AH_DIAG_SEC_1_CLASS,
            null as AH_DIAG_SEC_2,
            null as AH_DIAG_SEC_2_CLASS,
            null as AH_DIAG_SEC_3,
            null as AH_DIAG_SEC_3_CLASS,
            null as AH_DIAG_SEC_4,
            null as AH_DIAG_SEC_4_CLASS,
            null as AH_DIAG_SEC_5,
            null as AH_DIAG_SEC_5_CLASS,
            null as AH_DIAG_SEC_6,
            null as AH_DIAG_SEC_6_CLASS,
            null as AH_DIAG_SEC_7,
            null as AH_DIAG_SEC_7_CLASS,
            null as AH_DIAG_SEC_8,
            null as AH_DIAG_SEC_8_CLASS,
            null as AH_DIAG_SEC_9,
            null as AH_DIAG_SEC_9_CLASS,
            null as AH_PACIENTE_DADOS_VALIDADOS_CNS,
            null as AH_ST_INTERNACAO_CONCOM,
            null as AH_ST_INTERNACAO_CONCOM_BDNAIH,
            null as AH_STATUS_PR2,
            null as AH_STATUS_PR3,
            null as AH_STATUS_PR4,
            null as AH_STATUS_PR5,
            null as AH_STATUS_PR6,
            null as AH_STATUS_PR7,
            null as AH_PACIENTE_NUMERO_CPF,
            null as AH_ST_DUPLICIDADE_CPF
        from {{ source("brutos_sih_staging", "sih_2012_2014")}}

        union all
        
        select 
            -- SIH 2015 a 2023
            *,
            null as AH_PACIENTE_NUMERO_CPF,
            null as AH_ST_DUPLICIDADE_CPF
        from {{ source("brutos_sih_staging", "sih_2015_2023") }}

        union all
            -- SIH 2024
        select * from {{ source("brutos_sih_staging", "sih_2024") }}
    ),

    renomeado as (
        select
            {{ process_null('AH_CNES') }} as id_cnes,
            substr({{ process_null('AH_CMPT') }}, 1, 4) AS ano_cmpt,
            substr({{ process_null('AH_CMPT') }}, 5, 6) AS mes_cmpt,
            case 
                when ah_paciente_numero_cpf = "0"
                    then null
                when ah_paciente_numero_cpf = "00000000000"
                    then null
                else {{ process_null('AH_PACIENTE_NUMERO_CPF') }}
            end as paciente_cpf,
                        case
                when AH_PACIENTE_IDENT_DOC = '1'
                    then 'PIS/PASEP'
                when AH_PACIENTE_IDENT_DOC = '2'
                    then 'RG'
                when AH_PACIENTE_IDENT_DOC = '3'
                    then 'Certidão de Nascimento'
                when AH_PACIENTE_IDENT_DOC = '4'
                    then 'CPF'
                when AH_PACIENTE_IDENT_DOC = '5'
                    then 'Ignorado'
                else null
            end as paciente_ident_doc,
            case 
                when AH_PACIENTE_NUMERO_DOC like '00000000%' and AH_PACIENTE_IDENT_DOC = '4'
                    then substr(AH_PACIENTE_NUMERO_DOC, length(AH_PACIENTE_NUMERO_DOC) - 10)
                when AH_PACIENTE_NUMERO_DOC like '00000000%' and AH_PACIENTE_IDENT_DOC = '2'
                    then substr(AH_PACIENTE_NUMERO_DOC, length(AH_PACIENTE_NUMERO_DOC) - 8)
                when ah_paciente_numero_doc like " %" or AH_PACIENTE_IDENT_DOC = '5'
                    then null
                else {{ process_null('AH_PACIENTE_NUMERO_DOC') }}
            end as paciente_numero_doc,
            {{ process_null('AH_PRONTUARIO') }} as numero_prontuario,
            {{ process_null('AH_PACIENTE_NOME') }} as paciente_nome,
            case
                when length(ah_paciente_dt_nascimento) = 8
                    then cast(ah_paciente_dt_nascimento as date format 'YYYYMMDD')
                else null 
            end as paciente_data_nasc,
            {{ process_null('AH_PACIENTE_IDADE') }} as paciente_idade,
            {{ process_null('AH_PACIENTE_SEXO') }} as paciente_sexo,
            {{ process_null('AH_PACIENTE_RACA_COR') }} as paciente_raca_cor,
            {{ process_null('AH_PACIENTE_NOME_RESP') }} as paciente_nome_resp,
            {{ process_null('AH_PACIENTE_NOME_MAE') }} as paciente_nome_mae,
            case 
                when AH_PACIENTE_NUMERO_CNS like "00000%"
                    then null
                when AH_PACIENTE_NUMERO_CNS = '0'
                    then null
                else {{ process_null('AH_PACIENTE_NUMERO_CNS') }}
            end as paciente_cns,
            {{ process_null('AH_PACIENTE_NACIONALIDADE') }} as paciente_nacionalidade,
            {{ process_null('AH_PACIENTE_MUN_ORIGEM') }} as paciente_mun_origem,
            {{ process_null('AH_PACIENTE_TIPO_LOGR') }} as paciente_tipo_logr,
            {{ process_null('AH_PACIENTE_LOGR') }} as paciente_logr,
            {{ process_null('AH_PACIENTE_LOGR_NUMERO') }} as paciente_numero,
            nullif(trim(AH_PACIENTE_LOGR_COMPL), '') as paciente_complemento,
            {{ process_null('AH_PACIENTE_LOGR_BAIRRO') }} as paciente_bairro,
            {{ process_null('AH_PACIENTE_LOGR_MUNICIPIO') }} as paciente_municipio,
            {{ process_null('AH_PACIENTE_LOGR_UF') }} as paciente_uf,
            {{ process_null('AH_PACIENTE_LOGR_CEP') }} as paciente_cep,
            {{ process_null('AH_PACIENTE_ETNIA') }} as paciente_etnia,
            {{ process_null('AH_PACIENTE_TEL_DDD') }} as paciente_tel_ddd,
            {{ process_null('AH_PACIENTE_TEL_NUM') }} as paciente_tel_num,
            {{ process_null('AH_SEQ') }} as aih_sequencial_lote,
            case 
                when AH_SITUACAO = '0'
                    then 'Aprovada'
                when AH_SITUACAO = '1'
                    then 'Rejeitada'
                else null
            end as aih_situacao,
            {{ process_null('AH_LOTE') }} as lote,
            {{ process_null('AH_LOTE_APRES') }} as lote_apres,
            case 
                when AH_IDENT = '1' 
                    then 'Normal'
                when AH_IDENT = '3' 
                    then 'Continuação'
                when AH_IDENT = '4'
                    then 'Registro Civil'
                when AH_IDENT = '5'
                    then 'Longa permanência'
                else null
            end as aih_identificador,
            {{ process_null('AH_ESPECIALIDADE') }} as especialidade,
            {{ process_null('AH_NUM_AIH') }} as num_aih,
            if(AH_NUM_AIH_PROX in ("0000000000000", "0"), null, {{ process_null('AH_NUM_AIH_PROX') }}) as num_aih_prox,
            if(AH_NUM_AIH_ANT in ("0000000000000", "0"), null, {{ process_null('AH_NUM_AIH_ANT') }}) as num_aih_ant,
            if(AH_SEQ_AIH5 = '0', null, {{ process_null('AH_SEQ_AIH5') }}) as seq_aih5,
            {{ process_null('AH_OE_AIH') }} as orgao_emissor_aih,
            {{ process_null('AH_OE_GESTOR') }} as orgao_emissor_gestor,
            {{ process_null('AH_OE_REGIONAL') }} as orgao_emissor_regional,
            {{ process_null('AH_MUN_HOSP') }} as municipio_hospital,
            case
                when length(ah_dt_emissao) = 8
                    then cast(ah_dt_emissao as date format 'YYYYMMDD')
                when length(ah_dt_emissao) = 6
                    then cast(ah_dt_emissao as date format 'YYMMDD')
                else null
            end as data_emissao,
            safe_cast(ah_dt_internacao as date format 'YYYYMMDD') as data_internacao, 
            safe_cast(ah_dt_saida as date format 'YYYYMMDD') as data_saida, 
            {{ process_null('AH_PROC_SOLICITADO') }} as proc_solicitado,
            if(ah_st_muda_proc = '1', true, false) as mudanca_proc_indicador,
            {{ process_null('AH_PROC_REALIZADO') }} as proc_realizado,
            case 
                when ah_car_internacao = "01" 
                    then replace(ah_car_internacao, "01", "1")
                when ah_car_internacao = "02"
                    then replace(ah_car_internacao, "02", "2")
                else {{ process_null('AH_CAR_INTERNACAO') }}
            end as internacao_carater,
            case
                when AH_MODALIDADE_INTERNACAO = '02'
                    then 'Hospitalar'
                when AH_MODALIDADE_INTERNACAO = '03'
                    then 'Hospitalar dia'
                when AH_MODALIDADE_INTERNACAO = '04'
                    then 'Internação domiciliar'
                else null
            end as internacao_modalidade,
            {{ process_null('AH_MOT_SAIDA') }} as motivo_saida,
            case 
                when AH_MED_SOL_IDENT = '1'
                    then 'CPF'
                when AH_MED_SOL_IDENT = '2'
                    then 'CNS'
                else null
            end as med_sol_ident,
            if(ah_med_sol_doc = '0', null, {{ process_null('AH_MED_SOL_DOC') }} ) as med_sol_doc,
            case 
                when AH_MED_RESP_IDENT = '1'
                    then 'CPF'
                when AH_MED_RESP_IDENT = '2'
                    then 'CNS'
                else null
            end as med_resp_ident,
            if(ah_med_resp_doc = '0', null, {{ process_null('AH_MED_RESP_DOC') }} ) as med_resp_doc,
            case 
                when AH_DIR_CLINICO_IDENT = '1'
                    then 'CPF'
                when AH_DIR_CLINICO_IDENT = '2'
                    then 'CNS'
                else null
            end as dir_clinico_ident,
            if(ah_dir_clinico_doc = '0', null, {{ process_null('AH_DIR_CLINICO_DOC') }} ) as dir_clinico_doc,
            case 
                when AH_AUTORIZADOR_IDENT = '1'
                    then 'CPF'
                when AH_AUTORIZADOR_IDENT = '2'
                    then 'CNS'
                else null
            end as autorizador_ident,
            {{ process_null('AH_AUTORIZADOR_DOC') }} as autorizador_doc,
            if(AH_DIAG_PRI in ("0000", "0"), null, {{ process_null('AH_DIAG_PRI') }}) as diagnostico_principal,
            if(AH_DIAG_SEC in ("0000", "0"), null, {{ process_null('AH_DIAG_SEC') }}) as diagnostico_secundario,
            if(AH_DIAG_COMP in ("0000", "0"), null, {{ process_null('AH_DIAG_COMP') }}) as diagnostico_comp,
            if(AH_DIAG_OBITO in ("0000", "0"), null, {{ process_null('AH_DIAG_OBITO') }}) as diagnostico_obito,
            {{ process_null('AH_ENFERMARIA') }} as enfermaria,
            {{ process_null('AH_LEITO') }} as leito_numero,
            {{ process_null('AH_UTINEO_MOT_SAIDA') }} as utineo_motivo_saida,
            if(AH_UTINEO_PESO in ("0000", "0"), null, {{ process_null('AH_UTINEO_PESO') }}) as utineo_peso,
            {{ process_null('AH_UTINEO_MESES_GESTACAO') }} as utineo_meses_gestacao,
            case 
                when AH_ACDTRAB_CNPJ_EMP = '00000000000000'
                    then null
                when AH_ACDTRAB_CNPJ_EMP = '0'
                    then null
                else {{ process_null('AH_ACDTRAB_CNPJ_EMP') }}
            end as cnpj_empregador,
            case
                when AH_ACDTRAB_CBOR = '000000'
                    then null
                when AH_ACDTRAB_CBOR = '0'
                    then null
                else {{ process_null('AH_ACDTRAB_CBOR') }}
            end as id_cbo,
            case 
                when AH_ACDTRAB_CNAER = '000'
                    then null
                when AH_ACDTRAB_CNAER = '0'
                    then null
                else {{ process_null('AH_ACDTRAB_CNAER') }}
            end as id_cnaer,
            case 
                when AH_ACDTRAB_VINC_PREV = '1'
                    then 'Autônomo'
                when AH_ACDTRAB_VINC_PREV = '2'
                    then 'Desempregado'
                when AH_ACDTRAB_VINC_PREV = '3'
                    then 'Aposentado'
                when AH_ACDTRAB_VINC_PREV = '4'
                    then 'Não segurado'
                when AH_ACDTRAB_VINC_PREV = '5'
                    then 'Empregado'
                when AH_ACDTRAB_VINC_PREV = '6'
                    then 'Empregador'
                else null
            end as indicador_vinculo_previdencia,
            {{ process_null('AH_PARTO_QTD_NASC_VIVOS') }} as parto_nasc_vivos,
            {{ process_null('AH_PARTO_QTD_NASC_MORTOS') }} as parto_nasc_mortos,
            {{ process_null('AH_PARTO_QTD_ALTA') }} as parto_nasc_alta,
            {{ process_null('AH_PARTO_QTD_TRAN') }} as parto_transferidos,
            {{ process_null('AH_PARTO_QTD_OBITO') }} as parto_obito,
            case
                when AH_PARTO_NUM_PRENATAL in ('0', '000000000000') or REGEXP_CONTAINS(AH_PARTO_NUM_PRENATAL, r'^0{9}[,\.]*$')
                    then null
                else {{ process_null('AH_PARTO_NUM_PRENATAL') }}
            end as parto_id_prenatal,
            {{ process_null('AH_LAQVAS_QTD_FILHOS') }} as laqvas_filhos,
            case 
                when AH_LAQVAS_GRAU_INSTRUC = '1'
                    then 'Analfabeto'
                when AH_LAQVAS_GRAU_INSTRUC = '2'
                    then 'Primeiro grau'
                when AH_LAQVAS_GRAU_INSTRUC = '3'
                    then 'Segunda grau'
                when AH_LAQVAS_GRAU_INSTRUC = '4'
                    then 'Terceiro grau'
                else null
            end as laqvas_grau_instruc,
            if(AH_LAQVAS_CID_INDICACAO = '    ', null, {{ process_null('AH_LAQVAS_CID_INDICACAO')}}) as laqvas_cid_indicacao,
            {{ process_null('AH_LAQVAS_MET_CONTRACEP1') }} as laqvas_met_contracep1,
            {{ process_null('AH_LAQVAS_MET_CONTRACEP2') }} as laqvas_met_contracep2,
            if(ah_laqvas_gestacao_risco = "0", true, false) as laqvas_gestacao_risco_indicador,
            {{ process_null('AH_VERSAO_SISAIH01') }} as versao_sisaih01,
            if(ah_st_duplicidade = '1', true, false) as duplicidade_indicador,
            {{ process_null('AH_ST_BLOQUEIO') }} as bloqueio,
            case 
                when AH_ST_AGRAVO = '1'
                    then 'Bloqueada'
                when AH_ST_AGRAVO = '2'
                    then 'Cancelada'
                when AH_ST_AGRAVO = '3'
                    then 'Paga'
                when AH_ST_AGRAVO = '4'
                    then 'Reservada'
                else null
            end as agravo,
            {{ process_null('AH_MOT_BLOQ') }} as motivo_bloqueio,
            {{ process_null('AH_IN_GER_INF') }} as in_ger_inf,
            case 
                when AH_GESTOR_IDENT = '1'
                    then 'CPF'
                when AH_GESTOR_IDENT = '2'
                    then 'CNS'
                else null
            end as gestor_ident,
            if(AH_GESTOR_DOC in ("000000000000000", "0"), null, {{ process_null('AH_GESTOR_DOC') }}) as gestor_documento,
            {{ process_null('AH_COD_SOL_LIB') }} as cod_sol_lib,
            if(ah_st_into = "1", true, false) as into_indicador,
            case
                when AH_CONTRATO in ("0000", "0")
                    then null
                else {{ process_null('AH_CONTRATO') }}
            end as contrato,
            {{ process_null('AH_IVD_SH') }} as valorizacao_serv_hosp,
            {{ process_null('AH_IVD_SP') }} as valorizacao_serv_prest,
            {{ process_null('AH_DIARIAS') }} as diarias,
            {{ process_null('AH_DIARIAS_UTI') }} as diarias_uti,
            {{ process_null('AH_DIARIAS_UI') }} as diarias_ui,
            if(ah_st_cateterismo_anest = '1', true, false) as cateterismo_anest_indicador,
            {{ process_null('AH_COMPLEXIDADE') }} as complexidade,
            {{ process_null('AH_FINANCIAMENTO') }} as financiamento,
            {{ process_null('AH_TIPO_FAEC') }} as faec,
            {{ process_null('AH_ST_INTERNACAO_CONCOM') }} as internacao_concom,
            nullif(trim(AH_CS), '') as codigo_seguranca,
            {{ process_null('AH_AUDIT_SISAIH01_JUST') }} as audit_sisaih01_just,
            if(ah_st_duplicidade_cns = '1', true, false) as duplicidade_cns,
            {{ process_null('AH_PACIENTE_FONETICO_NOME') }} as paciente_fonetico_nome,
            {{ process_null('AH_PACIENTE_FONETICO_NOME_MAE') }} as paciente_fonetico_nome_mae,
            {% for i in range(1, 10) %}
                case 
                    when AH_DIAG_SEC_{{ i }} like ' %'
                        then null
                    else {{ process_null('AH_DIAG_SEC_' ~ i) }}
                end as diagsec{{ i }},
                {{ process_null('AH_DIAG_SEC_' ~ i ~ '_CLASS') }} as diagsec{{ i }}_class,
            {% endfor %}
            {{ process_null('AH_PACIENTE_DADOS_VALIDADOS_CNS') }} as paciente_dados_validados_cns,
            if(ah_st_duplicidade_cpf = '1', true, false) as duplicidade_cpf,
            {{ process_null('ROWNUMBER') }} as rownumber

        from sih_2008_2024
    )

select * from renomeado





