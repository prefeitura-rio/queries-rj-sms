{{
    config(
        alias="sisreg_solicitacoes_ressonancia_bacia_pelve",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}

with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_solicitacoes_ressonancia_bacia_pelve') }}
),
renamed as (
    select
        codigo_unidade_solicitante as id_estabelecimento_solicitante,
        nome_unidade_solicitante as estabelecimento_solicitante_nome,
        data_solicitacao as solicitacao_data,
        cid_solicitacao as cid_nome,
        codigo_cid_solicitado as id_cid,
        codigo_grupo_procedimento as id_grupo_procedimento,
        nome_grupo_procedimento as procedimento_grupo_nome,
        codigo_procedimento as id_procedimento,
        nome_procedimento as procedimento_nome,
        dt_justificativa_final as solicitacao_justificativa_final_data,
        codigo_tipo_vaga_solicitada as id_tipo_vaga_solicitada,
        risco as solicitacao_risco,
        data_cancelamento as solicitao_cancelamento_data,
        status_solicitacao as solicitacao_status,
        sigla_situacao as solicitacao_sigla_situacao,
        st_visualizado_regulador,
        codigo_tipo_fila as id_fila_tipo,
        codigo_tipo_regulacao as id_regulacao_tipo,
        nome_cnes_central_solicitante as estabelecimento_centro_solicitante_nome,
        codigo_cnes_central_solicitante as id_estabelecimento_central_solicitante,
        codigo_unidade_desejada as id_estabelecimento_desejado,
        nome_unidade_desejada as estabelecimento_desejado_nome,
        type,
        data_desejada as procedimento_data_desejada,
        data_atualizacao as solicitacao_data_atualizacao,
        id_usuario

    from source
)
select * from renamed
  