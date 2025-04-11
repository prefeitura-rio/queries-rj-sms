{{
    config(
        enabled=true,
        schema="brutos_sisreg_api",
        alias="solicitacoes",
    )
}}

with
    source as (
        select
            {{ process_null("codigo_grupo_procedimento") }}
            as codigo_grupo_procedimento,
            codigo_perfil_cancelamento,
            endereco_paciente_residencia,
            st_visualizado_regulador,
            nome_operador_solicitante,
            cns_usuario,
            bairro_paciente_residencia,
            codigo_uf_regulador,
            codigo_unidade_solicitante,
            no_mae_usuario,
            no_usuario,
            tipo_logradouro_paciente_residencia,
            telefone,
            nome_municipio_nascimento,
            numero_crm,
            procedimentos,
            codigo_solicitacao,
            municipio_paciente_residencia,
            nome_unidade_desejada,
            codigo_cnes_central_solicitante,
            sexo_usuario,
            nome_operador_videofonista,
            codigo_cid_solicitado,
            codigo_uf_solicitante,
            carga_epoch,
            codigo_tipo_fila,
            nome_cnes_central_solicitante,
            safe_cast(data_cancelamento as string) as data_cancelamento,
            cep_paciente_residencia,
            timestamp,
            safe_cast(data_atualizacao as string) as data_atualizacao,
            nome_medico_solicitante,
            uf_paciente_residencia,
            nome_grupo_procedimento,
            cpf_profissional_solicitante,
            nome_operador_cancelamento,
            safe_cast(data_solicitacao as string) as data_solicitacao,
            status_solicitacao,
            nome_perfil_cancelamento,
            codigo_tipo_regulacao,
            sigla_uf_solicitante,
            complemento_paciente_residencia,
            sigla_situacao,
            version,
            codigo_central_reguladora,
            descricao_cid_solicitado,
            codigo_classificacao_risco,
            uf_municipio_nascimento,
            safe_cast(data_desejada as string) as data_desejada,
            safe_cast(dt_nascimento_usuario as string) as dt_nascimento_usuario,
            numero_paciente_residencia,
            nome_central_reguladora,
            nome_central_solicitante,
            codigo_unidade_desejada,
            codigo_central_solicitante,
            codigo_tipo_vaga_solicitada,
            type,
            cpf_usuario,
            nome_unidade_solicitante,
            sigla_uf_regulador,
            laudo,
            safe_cast(data_extracao as string) as data_extracao,
            ano_particao,
            mes_particao,
            data_particao,
        from {{ source("brutos_sisreg_api_staging", "solicitacoes") }}
        where
            data_particao = (
                select max(data_particao)
                from {{ source("brutos_sisreg_api_staging", "solicitacoes") }}
            )
    )

select *
from source
