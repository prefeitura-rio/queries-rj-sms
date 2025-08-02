{{
    config(
        alias="exames_laboratoriais",
        materialized="table",
    )
}}

WITH

    source as (
        select *
        from {{ source("brutos_gal_staging", "exames_laboratoriais") }}
    ),
    dedup as (
        select *
        from source
        qualify row_number() over (partition by file_path, row_number order by loaded_at desc) = 1
    ),
    with_metadata as (
        select 
            REGEXP_REPLACE(split(file_path, '/')[safe_offset(5)], r'-\d{8}\.zip$', '') as tipo_exame,

            data,

            {{process_null('file_path')}} as caminho_arquivo,
            {{process_null('row_number')}} as indice_linha,
            {{process_null('loaded_at')}} as loaded_at
        from dedup
    ),
    cleared as (
        select
            tipo_exame,
            
            {{json_field(data, 'Requisição')}} as requisicao,
            {{json_field(data, 'Requisição Correlativo (S/N)')}} as requisicao_correlativo,
            {{json_field(data, 'Regional de Cadastro')}} as regional_cadastro,
            {{json_field(data, 'Laboratório de Cadastro')}} as laboratorio_cadastro,
            {{json_field(data, 'CNES Laboratório de Cadastro')}} as cnes_laboratorio_cadastro,
            {{json_field(data, 'Unidade Solicitante')}} as unidade_solicitante,
            {{json_field(data, 'CNES Unidade Solicitante')}} as cnes_unidade_solicitante,
            {{json_field(data, 'Municipio do Solicitante')}} as municipio_solicitante,
            {{json_field(data, 'IBGE Município Solicitante')}} as ibge_municipio_solicitante,
            {{json_field(data, 'Estado do Solicitante')}} as estado_solicitante,
            {{json_field(data, 'Nome Profissional de Saúde')}} as nome_profissional_saude,
            {{json_field(data, 'CNS do Profissional de Saúde')}} as cns_profissional_saude,
            {{json_field(data, 'Reg. Conselho/Matrícula')}} as reg_conselho_matricula,
            {{json_field(data, 'Agravo da Requisição')}} as agravo_requisicao,
            {{json_field(data, 'Data da Solicitação')}} as data_solicitacao,
            {{json_field(data, 'Data do 1º Sintomas')}} as data_primeiros_sintomas,
            {{json_field(data, 'Tomou Vacina')}} as tomou_vacina,
            {{json_field(data, 'Qual Vacina')}} as qual_vacina,
            {{json_field(data, 'Data Última Dose')}} as data_ultima_dose,
            {{json_field(data, 'Finalidade')}} as finalidade,
            {{json_field(data, 'Descrição Finalidade')}} as descricao_finalidade,
            {{json_field(data, 'Observação')}} as observacao,
            {{json_field(data, 'Núm. Notificação Sinan')}} as numero_notificacao_sinan,
            {{json_field(data, 'Agravo Sinan')}} as agravo_sinan,
            {{json_field(data, 'CID Agravo Sinan')}} as cid_agravo_sinan,
            {{json_field(data, 'Data Notificação Sinan')}} as data_notificacao_sinan,
            {{json_field(data, 'Unidade Notificação Sinan')}} as unidade_notificacao_sinan,
            {{json_field(data, 'CNES Unidade Notificação Sinan')}} as cnes_unidade_notificacao_sinan,
            {{json_field(data, 'Município Notificação Sinan')}} as municipio_notificacao_sinan,
            {{json_field(data, 'IBGE Município Notificação SINAN')}} as ibge_municipio_notificacao_sinan,
            {{json_field(data, 'Núm. Notificação Gal')}} as numero_notificacao_gal,
            {{json_field(data, 'Agravo Gal')}} as agravo_gal,
            {{json_field(data, 'CID Agravo Gal')}} as cid_agravo_gal,
            {{json_field(data, 'Data Notificação Gal')}} as data_notificacao_gal,
            {{json_field(data, 'Unidade Notificação Gal')}} as unidade_notificacao_gal,
            {{json_field(data, 'CNES Unidade Notificação Gal')}} as cnes_unidade_notificacao_gal,
            {{json_field(data, 'Município Notificação Gal')}} as municipio_notificacao_gal,
            {{json_field(data, 'IBGE Município Notificação Gal')}} as ibge_municipio_notificacao_gal,
            {{json_field(data, 'CNS do Paciente')}} as cns_paciente,
            {{json_field(data, 'Paciente')}} as paciente,
            {{json_field(data, 'Nome Social')}} as nome_social,
            {{json_field(data, 'Data de Nascimento')}} as data_nascimento,
            {{json_field(data, 'Idade')}} as idade,
            {{json_field(data, 'Tipo Idade')}} as tipo_idade,
            {{json_field(data, 'Sexo')}} as sexo,
            {{json_field(data, 'Idade Gestacional')}} as idade_gestacional,
            {{json_field(data, 'Nacionalidade')}} as nacionalidade,
            {{json_field(data, 'Raça/Cor')}} as raca_cor,
            {{json_field(data, 'Etnia')}} as etnia,
            {{json_field(data, 'Tipo Doc. Paciente 1')}} as tipo_doc_paciente_1,
            {{json_field(data, 'Documento Paciente 1')}} as documento_paciente_1,
            {{json_field(data, 'Tipo Doc. Paciente 2')}} as tipo_doc_paciente_2,
            {{json_field(data, 'Documento Paciente 2')}} as documento_paciente_2,
            {{json_field(data, 'Nome da Mãe')}} as nome_mae,
            {{json_field(data, 'Endereço')}} as endereco,
            {{json_field(data, 'Bairro')}} as bairro,
            {{json_field(data, 'CEP de Residência')}} as cep_residencia,
            {{json_field(data, 'Municipio de Residência')}} as municipio_residencia,
            {{json_field(data, 'IBGE Município de Residência')}} as ibge_municipio_residencia,
            {{json_field(data, 'Estado de Residência')}} as estado_residencia,
            {{json_field(data, 'País de Residência')}} as pais_residencia,
            {{json_field(data, 'Zona')}} as zona,
            {{json_field(data, 'Telefone de Contato')}} as telefone_contato,
            {{json_field(data, 'Nome da Pesquisa')}} as nome_pesquisa,
            {{json_field(data, 'Número Interno')}} as numero_interno,
            {{json_field(data, 'Exame')}} as exame,
            {{json_field(data, 'Metodologia')}} as metodologia,
            {{json_field(data, 'Exame Condicionado (S/N)')}} as exame_condicionado,
            {{json_field(data, 'Exame Restrito (S/N)')}} as exame_restrito,
            {{json_field(data, 'Exame Complementar (S/N)')}} as exame_complementar,
            {{json_field(data, 'Exame Correlativo (S/N)')}} as exame_correlativo,
            {{json_field(data, 'Data de Cadastro')}} as data_cadastro,
            {{json_field(data, 'Material Biológico')}} as material_biologico,
            {{json_field(data, 'Localização')}} as localizacao,
            {{json_field(data, 'Material Clínico')}} as material_clinico,
            {{json_field(data, 'Amostra')}} as amostra,
            {{json_field(data, 'Código da Amostra')}} as codigo_amostra,
            {{json_field(data, 'Data da Coleta')}} as data_coleta,
            {{json_field(data, 'Hora da Coleta')}} as hora_coleta,
            {{json_field(data, 'Usou Medicamento')}} as usou_medicamento,
            {{json_field(data, 'Medicamento')}} as medicamento,
            {{json_field(data, 'Data Uso Antibiótico')}} as data_uso_antibiotico,
            {{json_field(data, 'Kit')}} as kit,
            {{json_field(data, 'Fabricante')}} as fabricante,
            {{json_field(data, 'Lote do Kit')}} as lote_kit,
            {{json_field(data, 'Reteste')}} as reteste,
            {{json_field(data, 'Data do Encaminhamento')}} as data_encaminhamento,
            {{json_field(data, 'Data do Recebimento')}} as data_recebimento,
            {{json_field(data, 'Data Início do Processamento')}} as data_inicio_processamento,
            {{json_field(data, 'Data do Processamento')}} as data_processamento,
            {{json_field(data, 'Laboratório Responsável')}} as laboratorio_responsavel,
            {{json_field(data, 'CNES Laboratório responsável')}} as cnes_laboratorio_responsavel,
            {{json_field(data, 'Laboratório de Execução')}} as laboratorio_execucao,
            {{json_field(data, 'CNES Laboratório de Execução')}} as cnes_laboratorio_execucao,
            {{json_field(data, 'Data da Liberação')}} as data_liberacao,

            struct(
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Agente Etiológico')}}, null) as agente_etiologico,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Técnica')}}, null) as tecnica,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Bedaquilina')}}, null) as bedaquilina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Estreptomicina')}}, null) as estreptomicina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Ofloxacina')}}, null) as ofloxacina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Protionamida')}}, null) as protionamida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Capreomicina')}}, null) as capreomicina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Pretomanida')}}, null) as pretomanida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Etionamida')}}, null) as etionamida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Amicacina')}}, null) as amicacina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Cicloserina')}}, null) as cicloserina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Isoniazida')}}, null) as isoniazida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Etambutol')}}, null) as etambutol,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Delamanida')}}, null) as delamanida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Kanamicina')}}, null) as kanamicina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Rifampicina')}}, null) as rifampicina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Linezolida')}}, null) as linezolida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Pirazinamida')}}, null) as pirazinamida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Clofazimina')}}, null) as clo,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Levofloxacina')}}, null) as levofloxacina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Terizidona')}}, null) as terizidona,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Halo')}}, null) as halo,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Halo Isoniazida')}}, null) as halo_isoniazida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Halo Estreptomicina')}}, null) as halo_estreptomicina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Halo Pirazinamida')}}, null) as halo_pirazinamida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Halo Kanamicina')}}, null) as halo_kanamicina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Halo Etambutol')}}, null) as halo_etambutol,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Halo Rifampicina')}}, null) as halo_rifampicina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Halo Etionamida')}}, null) as halo_etionamida,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Moxifloxacina')}}, null) as moxifloxacina,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Moxifloxacina 0,25 ug/mL')}}, null) as moxifloxacina_0_25_ug_ml,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Moxifloxacina 1,0 ug/mL')}}, null) as moxifloxacina_1_0_ug_ml,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Outro Antibiótico 1')}}, null) as outro_antibiotico_1,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Resultado de Outro Antibiótico 1')}}, null) as resultado_outro_antibiotico_1,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Outro Antibiótico 2')}}, null) as outro_antibiotico_2,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Resultado de Outro Antibiótico 2')}}, null) as resultado_outro_antibiotico_2,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Outro Antibiótico 3')}}, null) as outro_antibiotico_3,
                IF(tipo_exame = 'tubt-tsa', {{json_field(data, 'Resultado de Outro Antibiótico 3')}}, null) as resultado_outro_antibiotico_3
            ) as tubt_tsa,

            struct(
                IF(tipo_exame = 'tubb-colzn', {{json_field(data, 'Aspecto da Amostra de Escarro')}}, null) as aspecto_amostra_escarro,
                IF(tipo_exame = 'tubb-colzn', {{json_field(data, 'Cultura prevista para')}}, null) as cultura_prevista,
                IF(tipo_exame = 'tubb-colzn', {{json_field(data, 'Resultado')}}, null) as resultado,
                IF(tipo_exame = 'tubb-colzn', {{json_field(data, 'Observações do Resultado')}}, null) as observacoes_resultado
            ) as tubb_colzn,
            
            struct(
                IF(tipo_exame = 'tugexp-pcrtr', {{json_field(data, 'Aspecto da Amostra de Escarro')}}, null) as aspecto_amostra_escarro,
                IF(tipo_exame = 'tugexp-pcrtr', {{json_field(data, 'Rifampicina')}}, null) as rifampicina,
                IF(tipo_exame = 'tugexp-pcrtr', {{json_field(data, 'Complemento')}}, null) as complemento,
                IF(tipo_exame = 'tugexp-pcrtr', {{json_field(data, 'DNA para o Complexo <i>Mycobacterium tuberculosis</i>')}}, null) as dna_para_complexo_mycobacterium_tuberculosis
            ) as tugexp_pcrtr,

            struct(
                IF(tipo_exame = 'tubc-culmb', {{json_field(data, 'Aspecto da Amostra de Escarro')}}, null) as aspecto_amostra_escarro,
                IF(tipo_exame = 'tubc-culmb', {{json_field(data, 'Método da Identificação')}}, null) as metodo_identificacao,
                IF(tipo_exame = 'tubc-culmb', {{json_field(data, 'Metodologia.1')}}, null) as metodologia_1,
                IF(tipo_exame = 'tubc-culmb', {{json_field(data, 'Técnica de Descontaminação')}}, null) as tecnica_descontaminacao,
                IF(tipo_exame = 'tubc-culmb', {{json_field(data, 'Teste de sensibilidade previsto para')}}, null) as teste_sensibilidade_previsto,
                IF(tipo_exame = 'tubc-culmb', {{json_field(data, 'Resultado')}}, null) as resultado,
                IF(tipo_exame = 'tubc-culmb', {{json_field(data, 'Espécie Identificada')}}, null) as especie_identificada,
                IF(tipo_exame = 'tubc-culmb', {{json_field(data, 'Identificação de Micobactéria prevista para')}}, null) as identificacao_micobacteria_prevista
            ) as tubc_culmb,

            struct(
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Agente Etiológico')}}, null) as agente_etiologico,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Técnica')}}, null) as tecnica,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Bedaquilina')}}, null) as bedaquilina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Estreptomicina')}}, null) as estreptomicina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Ofloxacina')}}, null) as ofloxacina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Capreomicina')}}, null) as capreomicina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Pretomanida')}}, null) as pretomanida,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Etionamida')}}, null) as etionamida,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Amicacina')}}, null) as amicacina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Cicloserina')}}, null) as cicloserina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Isoniazida')}}, null) as isoniazida,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Etambutol')}}, null) as etambutol,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Delamanida')}}, null) as delamanida,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Kanamicina')}}, null) as kanamicina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Rifampicina')}}, null) as rifampicina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Linezolida')}}, null) as linezolida,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Pirazinamida')}}, null) as pirazinamida,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Clofazimina')}}, null) as clofazimina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Levofloxacina')}}, null) as levofloxacina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Terizidona')}}, null) as terizidona,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Moxifloxacina')}}, null) as moxifloxacina,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Moxifloxacina 0,25 ug/mL')}}, null) as moxifloxacina_0_25_ug_ml,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Moxifloxacina 1,0 ug/mL')}}, null) as moxifloxacina_1_0_ug_ml,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Outro Antibiótico 1')}}, null) as outro_antibiotico_1,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Resultado de Outro Antibiótico 1')}}, null) as resultado_outro_antibiotico_1,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Outro Antibiótico 2')}}, null) as outro_antibiotico_2,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Resultado de Outro Antibiótico 2')}}, null) as resultado_outro_antibiotico_2,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Outro Antibiótico 3')}}, null) as outro_antibiotico_3,
                IF(tipo_exame = 'tubtii-tsa', {{json_field(data, 'Resultado de Outro Antibiótico 3')}}, null) as resultado_outro_antibiotico_3
            ) as tubtii_tsa,

            struct(
                IF(tipo_exame = 'tuber-elisa', {{json_field(data, 'CNES Laboratório Externo')}}, null) as cnes_laboratorio_externo,
                IF(tipo_exame = 'tuber-elisa', {{json_field(data, 'Resultado')}}, null) as resultado,
                IF(tipo_exame = 'tuber-elisa', {{json_field(data, 'Observações do Resultado')}}, null) as observacoes_resultado,
                IF(tipo_exame = 'tuber-elisa', {{json_field(data, 'D.O./C.O.')}}, null) as do_co
            ) as tuber_elisa,

            caminho_arquivo,
            indice_linha,
            loaded_at

        from with_metadata
    )
select * 
from cleared