{{
    config(
        schema="brutos_siclom_api",
        alias="carga_viral",
        tags=["siclom"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}


with

source as (select * from {{ source("brutos_siclom_api_staging", "carga_viral") }}),
    
carga_viral as (
    select 
        -- Identificação do paciente 
        {{ process_null('cd_pac') }} as id_paciente,
        {{ process_null('CPF') }} as cpf,
        {{ process_null('nm_pac') }} as paciente_nome,
        {{ process_null('nm_pac_social') }} as paciente_nome_social,
        {{ process_null('nm_mae') }} as paciente_mae_nome,
        {{ process_null('nm_resp') }} as paciente_responsavel_nome,

        -- Dados demográficos
        {{ process_null('sexo') }} as sexo,
        safe.parse_date('%d/%m/%Y', dt_nasc) as data_nascimento,
        {{ process_null('ds_escolaridade') }} as escolaridade,
        {{ process_null('ds_raca') }} as raca,

        -- Endereço
        {{ process_null('end_cont') }} as paciente_endereco,
        {{ process_null('bai_cont') }} as paciente_bairro,
        {{ process_null('cep_cont') }} as paciente_cep,
        {{ process_null('cd_uf') }} as paciente_uf,
        {{ process_null('nm_cid') }} as paciente_cidade,

        -- Dados do exame
        {{ process_null('num_form') }} as numero_formulario,
        {{ process_null('ident_amostra_lab') }} as id_amostra_laboratorial,
        {{ process_null('paciente_gestante') }} as paciente_gestante,
        safe_cast(nu_idade_gestacional as int64) as paciente_idade_gestacional,
        {{ process_null('ds_motivo_exame') }} as motivo_exame,
        {{ process_null('coleta_geno_simult') }} as coleta_genotipagem_simultanea, 
        {{ process_null('solic_geno_simultanea') }} as solicitacao_genotipagem_simultanea,

        -- Data e horários
        safe.parse_date('%d/%m/%Y', dt_sol_medico) as solicitacao_medica_data,
        safe.parse_date('%d/%m/%Y', data_coleta) as coleta_data,
        safe.parse_time('%H:%M', hora_coleta) as coleta_hora,
        safe.parse_date('%d/%m/%Y', data_rec_amostra) as recebimento_amostra_data,
        safe.parse_time('%H:%M', hora_rec_amostra) as recebimento_amostra_hora,
        safe.parse_date('%d/%m/%Y', data_exec_exame) as exame_execucao_data,
        safe.parse_date('%d/%m/%Y', dt_digit) as digitacao_data,
        safe.parse_date('%d/%m/%Y', data_libera_exame) as liberacao_exame_data,
        safe.parse_time('%H:%M', hora_libera_exame) as liberacao_exame_hora,
        safe.parse_time('%H:%M', dt_inc) as inclusao_data,

        -- Profissional solicitante
        {{ process_null('autorizado_digitador_solicitacao') }} as solicitacao_digitador,
        {{ process_null('ds_tipo_profissional') }} as profissional_tipo,
        {{ process_null('nu_conselho') }} as id_conselho,
        {{ process_null('sg_uf_conselho') }} as conselho_uf,

        -- Instituições
        {{ process_null('instituicao_solicitante') }} as instituicao_solicitante,
        {{ process_null('uf_instituicao_solicitante') }} as instituicao_solicitante_uf,
        {{ process_null('cidade_instituicao_solicitante') }} as instituicao_solicitante_cidade,
        {{ process_null('instituicao_coletora') }} as instituicao_coletora, 
        {{ process_null('uf_instituicao_coletora') }} as instituicao_coletora_uf,
        {{ process_null('cidade_instituicao_coletora') }} as instituicao_coletora_cidade,
        {{ process_null('instituicao_executora') }} as instituicao_executora,
        {{ process_null('uf_instituicao_executora') }} as instituicao_executora_uf,
        {{ process_null('cidade_instituicao_executora') }} as instituicao_executora_cidade, 

        -- Resultados do exame
        {{ process_null('copias') }} as copias,
        safe_cast(`log` as float64) as `log`,
        safe_cast(volume_amostra as int64) as volume_amostra,

        -- Método e Kit
        {{ process_null('nm_metodo') }} as metodo,
        {{ process_null('nm_kit') }} as kit_nome,
        {{ process_null('tipo_entrada') }} as tipo_entrada,

        -- Outros
        {{ process_null('autorizado_digitador_exame') }} as exame_digitador,
        {{ process_null('observacoes') }} as observacoes,
        {{ process_null('autorizado_liberador') }} as liberador,
        {{ process_null('ds_projeto') }} as projeto,

        cast(extracted_at as datetime) as extraido_em,
        date(data_particao) as data_particao
    from source
    )

select 
    *
from carga_viral
