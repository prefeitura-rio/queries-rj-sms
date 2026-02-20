{{
    config(
        schema="brutos_siclom_api",
        alias="carga_cd4",
        tags=["siclom"],
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

with 

source as (
    select * from {{ source('brutos_siclom_api_staging', 'carga_cd4') }}
),

linfocitos_cd4 as (
    select
        -- Identificação do paciente 
        {{ process_null('cd_pac') }} as id_cidadao,
        {{ process_null('CPF') }} as paciente_cpf,
        {{ process_null('nm_pac') }} as paciente_nome,
        {{ process_null('nm_pac_social') }} as paciente_nome_social,
        {{ process_null('nm_mae') }} as paciente_mae_nome,
        {{ process_null('nm_resp') }} as paciente_responsavel_nome,

        -- Dados demográficos
        {{ process_null('sexo') }} as sexo,
        {{ process_null('ds_escolaridade') }} as escolaridade,
        {{ process_null('ds_raca') }} as raca,
        safe.parse_date('%d/%m/%Y', dt_nasc) as data_nascimento,

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

        -- Avaliação clínica
        {{ process_null('st_dois_ult_cd4_maior_350') }} as dois_ult_cd4_maior_350,
        {{ process_null('estagio_clinico') }} as estagio_clinico,
        {{ process_null('st_carga_viral_indetectavel') }} as carga_viral_indentectavel,
        {{ process_null('avaliacao_inicial') }} as avaliacao_inicial,
        {{ process_null('mo_pes_assinto_segmento') }} as pessoa_assintomatica_segmento,
        {{ process_null('mo_crianca_adolescente') }} as crianca_adolescente,
        {{ process_null('mo_pes_falha_viro') }} as pessoa_falha_virologica,
        {{ process_null('mo_pes_sinto') }} as pessoa_sintomatica,
        {{ process_null('avaliacao_imuniza') }} as avaliacao_imuniza,
        {{ process_null('avaliacao_pes_perda_seg') }} as avaliacao_pes_perda_seg,
        {{ process_null('nao_informado') }} as nao_informado, 

        -- Datas e horários
        safe.parse_date('%d/%m/%Y', dt_sol_medico) as solicitacao_medica_data,
        safe.parse_date('%d/%m/%Y', data_coleta) as coleta_data,
        safe.parse_time('%H:%M', hora_coleta) as coleta_hora,
        safe.parse_date('%d/%m/%Y', data_rec_amostra) as recebimento_amostra_data,
        safe.parse_time('%H:%M', hora_rec_amostra) as recebimento_amostra_hora,
        safe.parse_date('%d/%m/%Y', data_exec_exame) as exame_execucao_data,
        safe.parse_date('%d/%m/%Y', dt_digit) as digitacao_data,
        safe.parse_date('%d/%m/%Y', dt_dig_res) as digitacao_resultado_data,
        safe.parse_date('%d/%m/%Y', data_libera_exame) as liberacao_exame_data,
        safe.parse_time('%H:%M', hora_libera_exame) as liberacao_exame_hora,

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
        safe_cast(contagem_cd4 as int64) as cd4_contagem,
        safe_cast(perc_cd4 as float64) as cd4_percentual,
        safe_cast(contagem_cd8 as int64) as cd8_contagem,
        safe_cast(perc_cd8 as float64) as cd8_percentual,
        safe_cast(contagem_cd3 as int64) as cd3_contagem,
        safe_cast(linfocitos as int64) as linfocitos,

        -- Método e Kit
        {{ process_null('nm_metodo') }} as metodo,
        {{ process_null('nm_kit') }} as kit_nome,
        {{ process_null('tipo_entrada') }} as tipo_entrada,

        -- Outros
        {{ process_null('autorizado_digitador_exame') }} as digitador_exame,
        {{ process_null('observacoes') }} as observacoes,
        {{ process_null('autorizado_liberador') }} as liberador, 
        {{ process_null('ds_projeto') }} as projeto,

        cast(extracted_at as datetime) as extraido_em,
        date(data_particao) as data_particao
    from source
)

select 
    *
from linfocitos_cd4