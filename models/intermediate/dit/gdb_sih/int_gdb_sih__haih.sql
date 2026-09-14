{{
    config(
        schema="intermediario_gdb_sih",
        alias="haih",
        materialized="table",
        tags=["gdb_sih"]
    )
}}


with
    dedup as (
        select

            cast(id_sequencial as int64) as id_sequencial,
            nullif(id_sequencial_longa_permanencia, "000") as id_sequencial_longa_permanencia,

            case situacao
                when "0" then "aprovada"
                when "1" then "rejeitada"
                else situacao
            end as situacao,

            lote,
            parse_date("%Y%m", nullif(lote_apresentacao, "999999")) as lote_apresentacao_data,

            case trim(tipo_identificador_aih)
                when "01" then "normal"
                -- não tem 02 mesmo
                when "03" then "de continuacao"
                when "04" then "de registro civil"
                when "05" then "de longa permanencia"
                else trim(tipo_identificador_aih)
            end as tipo_identificador_aih,
            codigo_especialidade,

            numero_aih,
            numero_aih_anterior,
            numero_aih_proximo,

            parse_date("%Y%m", nullif(competencia, "999999")) as competencia,

            emissor_aih,
            emissor_gestor,
            emissor_regional
            id_cnes,
            municipio_hospital,  -- IBGE

            parse_date("%Y%m%d", nullif(data_emissao, "99999999")) as data_emissao,
            parse_date("%Y%m%d", nullif(data_internacao, "99999999")) as data_internacao,
            parse_date("%Y%m%d", nullif(data_saida, "99999999")) as data_saida,

            -- TU_PROCEDIMENTO
            procedimento_solicitado,
            procedimento_realizado,

            carater_internacao,  -- C_D 0007? 0008?
            modalidade_internacao,
            motivo_saida,  -- creio que C_D tabela 0023

            medico_solicitante_ident,  -- creio que C_D tabela 0033 (mas lista CNS como "RG"?)
            medico_solicitante_doc,
            medico_responsavel_ident,  -- C_D 0033
            medico_responsavel_doc
            diretor_clinico_ident,  -- C_D 0033
            diretor_clinico_doc,
            autorizador_ident,  -- C_D 0033
            autorizador_doc,

            paciente_nome,
            parse_date("%Y%m%d", nullif(paciente_data_nascimento, "99999999")) as paciente_data_nascimento,
            paciente_idade,
            case lower(trim(paciente_sexo))
                when "m" then "masculino"
                when "f" then "feminino"
                else lower(trim(paciente_sexo))
            end as paciente_sexo,
            paciente_raca,  -- creio que C_D tabela 0026

            paciente_nome_responsavel,
            paciente_nome_mae,
            paciente_ident,  -- C_D 0033
            paciente_doc,
            paciente_etnia,  -- creio que C_D tabela 0034

            paciente_tel_ddd,
            paciente_tel_numero,
            paciente_numero_cpf,
            paciente_numero_cns,
            paciente_situacao_rua,  -- S (sim), N (não), I (ignorado?), A (?)
            paciente_nacionalidade,  -- creio que C_D tabela 0029
            paciente_municipio_origem,  -- IBGE? parece sempre nulo

            paciente_tipo_logradouro,  -- creio que C_D tabela 0021
            paciente_logradouro,
            paciente_logradouro_numero,
            paciente_logradouro_complemento,
            nullif(paciente_logradouro_bairro, "INDEFINIDO") as paciente_logradouro_bairro,
            paciente_logradouro_municipio,  -- IBGE
            paciente_logradouro_uf,
            paciente_logradouro_cep,

            prontuario as prontuario_numero,
            enfermaria,  -- id?
            leito,  -- id?

            cid_diagnostico_principal,
            cid_diagnostico_secundario,
            cid_diagnostico_comp,
            cid_diagnostico_obito,

            cid_diagnostico_secundario_1,
            cid_diag_sec_1_class,
            cid_diagnostico_secundario_2,
            cid_diag_sec_2_class,
            cid_diagnostico_secundario_3,
            cid_diag_sec_3_class,
            cid_diagnostico_secundario_4,
            cid_diag_sec_4_class,
            cid_diagnostico_secundario_5,
            cid_diag_sec_5_class,
            cid_diagnostico_secundario_6,
            cid_diag_sec_6_class,
            cid_diagnostico_secundario_7,
            cid_diag_sec_7_class,
            cid_diagnostico_secundario_8,
            cid_diag_sec_8_class,
            cid_diagnostico_secundario_9,
            cid_diag_sec_9_class,

            parto_quantidade_nascidos_vivos,
            parto_quantidade_nascidos_mortos,
            parto_quantidade_alta,
            parto_quantidade_transferidos,
            parto_quantidade_obito,
            nullif(gestante_numero_prenatal, "000000000000") as gestante_numero_prenatal,

            utineo_motivo_saida,
            nullif(utineo_peso, "0000") as utineo_peso,
            utineo_meses_gestacao,

            cnpj_empresa,
            nullif(id_cbo_paciente, "000000") as id_cbo_paciente,
            nullif(id_cnae, "000") as id_cnae,

            possui_vinculo_previdencia,  -- sempre '0'?

            safe_cast(laqvas_quantidade_filhos as int64) as laqvas_quantidade_filhos,
            laqvas_grau_instrucao,  -- 0-4
            laqvas_cid_indicacao,
            laqvas_metodo_contraceptivo_1,  -- 00-13?
            laqvas_metodo_contraceptivo_2,  -- 00-13?
            case lower(trim(laqvas_gestacao_risco))
                when "0" then "nao"
                when "1" then "sim"
                else lower(trim(laqvas_gestacao_risco))
            end as laqvas_gestacao_risco,

            st_muda_proc,
            st_duplicidade,
            st_duplicidade_cpf,
            st_duplicidade_cns,
            st_bloqueio,
            st_agravo,
            st_inst_traumato_ortopedia,
            st_cateterismo_anest,
            st_mental,
            st_ortopedia,
            st_neurologia,
            st_oncologia,
            st_internacao_concom,
            st_internacao_concom_bdnaih,

            status_pr,
            status_pr1,
            status_pr2,
            status_pr3,
            status_pr4,
            status_pr5,
            status_pr6,
            status_pr7,

            safe_cast(diarias as int64) as diarias,
            safe_cast(diarias_uti as int64) as diarias_uti,
            safe_cast(diarias_ui as int64) as diarias_ui,

            gestor_ident,  -- C_D 0033
            nullif(gestor_doc, "000000000000000") as gestor_doc,

            motivo_bloqueio,
            in_ger_inf,  -- sempre "*"?

            nullif(codigo_sol_lib, "00000") as codigo_sol_lib,
            nullif(contrato, "0000") as contrato,
            safe_cast(nullif(ivd_servicos_hospitalares, "0.00") as float64) as ivd_servicos_hospitalares,
            safe_cast(nullif(ivd_servicos_profissionais, "0.00") as float64) as ivd_servicos_profissionais,

            complexidade,  -- creio que C_D 0025
            financiamento,  -- ?
            tipo_faec,
            versao_sisaih01,
            audit_just,
            audit_sisaih01_just,

            paciente_dados_validados_cns,  -- 0 ou 1

            data_particao,
            data_carga

        from {{ ref("raw_gdb_sih__haih") }}
        where data_particao = (
            select max(data_particao)
            from {{ ref("raw_gdb_sih__haih") }}
        )
        qualify row_number() over (
            partition by lote, id_sequencial
            order by data_carga desc
        ) = 1
    )

select *
from dedup
