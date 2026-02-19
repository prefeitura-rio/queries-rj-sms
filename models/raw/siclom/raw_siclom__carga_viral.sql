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

-- TODO: Confirmar nome das colunas com a SAP

with
    source as (select * from {{ source("brutos_siclom_api_staging", "carga_viral") }}),
    
    carga_viral as (
    select 
        {{ process_null('ident_amostra_lab') }} as id_amostra_laboratorial,
        {{ process_null('cd_pac') }} as id_paciente,
        {{ process_null('nm_pac') }} as paciente_nome,
        {{ process_null('CPF') }} as cpf,
        {{ process_null('nm_pac_social') }} as paciente_nome_social,
        {{ process_null('nm_mae') }} as mae_nome,
        {{ process_null('nm_resp') }} as responsavel_nome,
        {{ process_null('sexo') }} as sexo,
        {{ process_null('ds_escolaridade') }} as ds_escolaridade,
        {{ process_null('dt_nasc') }} as data_nascimento,
        {{ process_null('end_cont') }} as paciente_endereco,
        {{ process_null('bai_cont') }} as paciente_bairro,
        {{ process_null('cep_cont') }} as paciente_cep,
        {{ process_null('cd_uf') }} as paciente_uf,
        {{ process_null('nm_cid') }} as paciente_cidade,
        {{ process_null('num_form') }} as numero_formulario,
        {{ process_null('paciente_gestante') }} as paciente_gestante,
        {{ process_null('nu_idade_gestacional') }} as paciente_idade_gestacional,
        {{ process_null('ds_motivo_exame') }} as motivo_exame,
        {{ process_null('coleta_geno_simult') }} as coleta_geno_simult, 
        {{ process_null('solic_geno_simultanea') }} as solic_geno_simultanea,
        {{ process_null('dt_sol_medico') }} as data_solicitacao_medico,
        {{ process_null('data_coleta') }} as data_coleta,
        {{ process_null('hora_coleta') }} as hora_coleta,
        {{ process_null('data_rec_amostra') }} as data_rec_amostra,
        {{ process_null('hora_rec_amostra') }} as hora_rec_amostra,
        {{ process_null('data_exec_exame') }} as data_exec_exame,
        {{ process_null('hora_exec_exame') }} as hora_exec_exame,
        {{ process_null('dt_digit') }} as data_digitalizacao,
        {{ process_null('autorizado_digitador_solicitacao') }} as autorizado_digitador_solicitacao,
        {{ process_null('ds_tipo_profissional') }} as ds_tipo_profissional,
        {{ process_null('nu_conselho') }} as id_conselho,
        {{ process_null('sg_uf_conselho') }} as sigla_uf_conselho,
        {{ process_null('instituicao_solicitante') }} as instituicao_solicitante,
        {{ process_null('uf_instituicao_solicitante') }} as instituicao_solicitante_uf,
        {{ process_null('cidade_instituicao_solicitante') }} as instituicao_solicitante_cidade,
        {{ process_null('instituicao_coletora') }} as instituicao_coletora,
        {{ process_null('instituicao_executora') }} as instituicao_executora,
        {{ process_null('uf_instituicao_executora') }} as instituicao_executora_uf,
        {{ process_null('copias') }} as copias,
        {{ process_null('log') }} as log,
        {{ process_null('volume_amostra') }} as volume_amostra,
        {{ process_null('nm_metodo') }} as metodo,
        {{ process_null('nm_kit') }} as kit_nome,
        {{ process_null('tipo_entrada') }} as tipo_entrada,
        {{ process_null('dt_inc') }} as dt_inc, -- Confirmar o que seria esta data
        {{ process_null('autorizado_digitador_exame') }} as autorizado_digitador_exame,
        {{ process_null('observacoes') }} as observacoes,
        {{ process_null('data_libera_exame') }} as liberacao_exame_data,
        {{ process_null('hora_libera_exame') }} as liberacao_exame_hora,
        {{ process_null('autorizado_liberador') }} as autorizado_liberador,
        {{ process_null('ds_projeto') }} as projeto,
        {{ process_null('extracted_at') }} as extraido_em,
        date(data_particao) as data_particao
    from source
    )

select 
    *,
    safe_cast(cpf as int64) as cpf_particao
from carga_viral
