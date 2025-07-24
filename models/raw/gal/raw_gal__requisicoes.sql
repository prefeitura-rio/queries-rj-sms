{{
    config(
        alias="requisicoes",
        materialized="table",
    )
}}

with
    source as (
        select * from {{ source("brutos_gal_staging", "requisicoes_2020-2024") }}
    ),
    cleared as (
        select

            {{process_null('id')}} as id,
            {{process_null('id_requisicao')}} as id_requisicao,
            {{process_null('resultado')}} as id_tipo_resultado,
            {{process_null('agravo')}} as agravo_descricao,

            {{process_null('resultado')}} as resultado,
            data_solicitacao as data_solicitacao,
            data_1sintomas as data_primeiros_sintomas,

            {{process_null('tomouvacina')}} as vacina_paciente_tomou,
            {{process_null('nome_vacina')}} as nome_vacina,
            data_ultima_dose_vacina as vacina_data_ultima_dose,

            {{process_null('finalidade')}} as finalidade,
            {{process_null('descr_finalidade')}} as finalidade_descricao,
            {{process_null('obs')}} as observacoes,

            {{process_null('notificacao_sinam')}} as sinan_notificacao,
            {{process_null('agravo_sinam')}} as sinan_agravo_id,
            {{process_null('cid_sinam')}} as sinan_cid_id,
            data_notificacao_sinam as sinan_data_notificacao,
            {{process_null('unidade_sinam')}} as sinan_unidade_id,
            {{process_null('cnes_unidade_sinam')}} as sinan_cnes_unidade,
            {{process_null('municipio_unidade_sinam')}} as sinan_municipio_unidade,
            {{process_null('ibge_unidade_sinam')}} as sinan_ibge_unidade,

            {{process_null('notificacao_gal')}} as gal_notificacao,
            {{process_null('agravo_gal')}} as gal_agravo_id,
            {{process_null('cid_gal')}} as gal_cid_id,
            data_notificacao_gal as gal_data_notificacao,
            {{process_null('unidade_gal')}} as gal_unidade_id,
            {{process_null('cnes_unidade_gal')}} as gal_cnes_unidade,
            {{process_null('municipio_gal')}} as gal_municipio,
            {{process_null('ibge_municipio_gal')}} as gal_ibge_municipio,

            {{process_null('cns_paciente')}} as paciente_cns,
            {{process_null('nome_paciente')}} as paciente_nome,
            data_nascimento as paciente_data_nascimento,
            idade as paciente_idade,
            {{process_null('tipo_idade')}} as paciente_tipo_idade,
            {{process_null('sexo')}} as paciente_sexo,
            {{process_null('idade_gestacional')}} as paciente_idade_gestacional,
            {{process_null('nacionalidade')}} as paciente_nacionalidade,
            {{process_null('raca_cor')}} as paciente_raca_cor,
            {{process_null('etnia')}} as paciente_etnia,
            {{process_null('nome_mae')}} as paciente_nome_mae,
            {{process_null('doc_1')}} as paciente_documento_1,
            {{process_null('tipo_doc_1')}} as paciente_tipo_documento_1,
            {{process_null('doc_2')}} as paciente_documento_2,
            {{process_null('tipo_doc_2')}} as paciente_tipo_documento_2,
            {{process_null('endereco_paciente')}} as paciente_endereco,
            {{process_null('bairro')}} as paciente_endereco_bairro,
            {{process_null('cep_residencia')}} as paciente_endereco_cep,
            {{process_null('municipio_residencia')}} as paciente_endereco_municipio,
            {{process_null('ibge_municipio_residencia')}} as paciente_endereco_ibge_municipio,
            {{process_null('estado_residencia')}} as paciente_endereco_estado,
            {{process_null('pais_residencia')}} as paciente_endereco_pais,
            {{process_null('zona')}} as paciente_zona,
            {{process_null('telefone')}} as paciente_telefone,

            {{ process_null('requisicao_correlativo') }} as requisicao_correlativo,
            {{ process_null('unidade_solicitante') }} as unidade_solicitante,
            {{ process_null('cnes_unidade_solicitante') }} as cnes_unidade_solicitante,
            {{ process_null('municipio_solicitante') }} as municipio_solicitante,
            {{ process_null('ibge_municipio_solicitante') }} as ibge_municipio_solicitante,
            {{ process_null('estado_solicitante') }} as estado_solicitante,

            {{ process_null('cns_profissional') }} as profissional_cns,
            {{ process_null('nome_profissional') }} as profissional_nome,
            {{ process_null('doc_profissional') }} as profissional_documento,

            {{process_null('num_interno')}} as numero_interno,
            {{process_null('exame')}} as exame,
            {{process_null('metodologia')}} as metodologia,
            {{process_null('exame_condicionado')}} as exame_condicionado,
            {{process_null('exame_restrito')}} as exame_restrito,
            {{process_null('exame_complementar')}} as exame_complementar,
            {{process_null('exame_correlativo')}} as exame_correlativo,
            data_cadastro as data_cadastro,
            regional_cadastro as regional_cadastro,
            {{process_null('material_biologico')}} as material_biologico,
            {{process_null('localizacao')}} as material_localizacao,
            {{process_null('material_clinico')}} as material_clinico,
            {{process_null('amostra')}} as material_amostra,
            {{process_null('cod_amostra')}} as material_amostra_codigo,
            data_coleta as material_data_coleta,
            {{process_null('hora_coleta')}} as material_hora_coleta,

            {{process_null('usou_medicamento')}} as medicamento_paciente_usou,
            {{process_null('medicamento')}} as medicamento_nome,
            data_uso_antibiotico as antibiotico_data_uso,

            {{process_null('kit')}} as kit_descricao,
            {{process_null('fabricante')}} as kit_fabricante,
            {{process_null('lote_kit')}} as kit_lote,
            {{process_null('reteste')}} as kit_reteste,

            data_encaminhamento as data_encaminhamento,
            data_recebimento as data_recebimento,
            data_inicio_processamento as data_inicio_processamento,
            data_processamento as data_processamento,
            {{process_null('laboratorio_responsavel')}} as laboratorio_responsavel,
            laboratorio_cadastro as laboratorio_cadastro,
            {{process_null('cnes_laboratorio_cadastro')}} as cnes_laboratorio_cadastro,
            {{process_null('cnes_laboratorio_responsavel')}} as cnes_laboratorio_responsavel,
            {{process_null('laboratorio_execucao')}} as laboratorio_execucao,
            {{process_null('cnes_laboratorio_execucao')}} as cnes_laboratorio_execucao,
            data_liberacao as data_liberacao,
            {{process_null('status_exame')}} as status_exame
        from source
    )

select * from cleared