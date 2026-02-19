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

-- TODO: Confirmar nome das colunas com a SAP

with 

source as (
    select * from {{ source('brutos_siclom_api_staging', 'carga_cd4') }}
),

    linfocitos_cd4 as (
        select
            {{ process_null('CPF') }} as paciente_cpf,
            {{ process_null('cd_pac') }} as id_cidadao,
            {{ process_null('nm_pac') }} as paciente_nome,
            {{ process_null('nm_pac_social') }} as paciente_nome_social,
            {{ process_null('nm_mae') }} as mae_nome,
            {{ process_null('nm_resp') }} as responsavel_nome,
            {{ process_null('sexo') }} as sexo,
            {{ process_null('ds_escolaridade') }} as escolaridade,
            {{ process_null('ds_raca') }} as raca,
            {{ process_null('dt_nasc') }} as data_nascimento,
            {{ process_null('end_cont') }} as paciente_endereco,
            {{ process_null('bai_cont') }} as paciente_bairro,
            {{ process_null('cep_cont') }} as paciente_cep,
            {{ process_null('cd_uf') }} as paciente_uf,
            {{ process_null('nm_cid') }} as paciente_cidade,
            {{ process_null('num_form') }} as numero_formulario,
            {{ process_null('ident_amostra_lab') }} as id_amostra_laboratorial,
            {{ process_null('paciente_gestante') }} as paciente_gestante_indicador,
            {{ process_null('nu_idade_gestacional') }} as paciente_idade_gestacional,
            {{ process_null('ds_motivo_exame') }} as motivo_exame,
            {{ process_null('st_dois_ult_cd4_maior_350') }} as dois_ult_cd4_maior_350,
            {{ process_null('estagio_clinico') }} as estagio_clinico,
            {{ process_null('st_carga_viral_indetectavel') }} as carga_viral_indentectavel,
            {{ process_null('avaliacao_inicial') }} as avaliacao_inicial,
            {{ process_null('mo_pes_assinto_segmento') }} as mo_pes_assinto_segmento,
            {{ process_null('mo_crianca_adolescente') }} as mo_crianca_adolescente,
            {{ process_null('mo_pes_falha_viro') }} as mo_pes_falha_viro,
            {{ process_null('mo_pes_sinto') }} as mo_pes_sinto,
            {{ process_null('avaliacao_imuniza') }} as avaliacao_imuniza,
            {{ process_null('avaliacao_pes_perda_seg') }} as avaliacao_pes_perda_seg,
            {{ process_null('nao_informado') }} as nao_informado, 
            {{ process_null('dt_sol_medico') }} as data_solicitacao_medico,
            {{ process_null('data_coleta') }} as data_coleta,
            {{ process_null('hora_coleta') }} as hora_coleta,
            {{ process_null('data_rec_amostra') }} as data_rec_amostra, 
            {{ process_null('hora_rec_amostra') }} as hora_rec_amostra, 
            {{ process_null('data_exec_exame') }} as data_exec_exame, 
            {{ process_null('dt_digit') }} as dt_digit,
            {{ process_null('autorizado_digitador_solicitacao') }} as autorizado_digitador_solicitacao, 
            {{ process_null('ds_tipo_profissional') }} as tipo_profissional,
            {{ process_null('nu_conselho') }} as numero_conselho,
            {{ process_null('sg_uf_conselho') }} as sigla_uf_conselho,
            {{ process_null('instituicao_solicitante') }} as instituicao_solicitante,
            {{ process_null('uf_instituicao_solicitante') }} as uf_instituicao_solicitante,
            {{ process_null('cidade_instituicao_solicitante') }} as cidade_instituicao_solicitante,
            {{ process_null('instituicao_coletora') }} as instituicao_coletora, 
            {{ process_null('uf_instituicao_coletora') }} as uf_instituicao_coletora,
            {{ process_null('cidade_instituicao_coletora') }} as cidade_instituicao_coletora,
            {{ process_null('instituicao_executora') }} as instituicao_executora,
            {{ process_null('uf_instituicao_executora') }} as uf_instituicao_executora,
            {{ process_null('cidade_instituicao_executora') }} as cidade_instituicao_executora, 
            {{ process_null('contagem_cd4') }} as cd4_contagem,
            {{ process_null('perc_cd4') }} as cd4_percentual,
            {{ process_null('contagem_cd8') }} as cd8_contagem,
            {{ process_null('perc_cd8') }} as cd8_percentual,
            {{ process_null('contagem_cd3') }} as cd3_contagem,
            {{ process_null('linfocitos') }} as linfocitos,
            {{ process_null('nm_metodo') }} as metodo,
            {{ process_null('nm_kit') }} as kit_nome,
            {{ process_null('tipo_entrada') }} as tipo_entrada,
            {{ process_null('dt_dig_res') }} as dt_dig_res,
            {{ process_null('autorizado_digitador_exame') }} as autorizado_digitador_exame,
            {{ process_null('observacoes') }} as observacoes,
            {{ process_null('data_libera_exame') }} as data_liberacao_exame,
            {{ process_null('hora_libera_exame') }} as hora_liberacao_exame,
            {{ process_null('autorizado_liberador') }} as autorizado_liberador, 
            {{ process_null('ds_projeto') }} as projeto,
            {{ process_null('extracted_at') }} as extraido_em,
            date(data_particao) as data_particao
        from source
    )

select 
    *,
    safe_cast(paciente_cpf as int64) as cpf_particao
from linfocitos_cd4