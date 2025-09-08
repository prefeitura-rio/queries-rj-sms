-- noqa: disable=LT08
{{
  config(
    enabled=true,
    schema="saude_sisreg",
    alias="solicitacoes",
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='solicitacao_id',
    partition_by={
      "field": "data_solicitacao",
      "data_type": "date",
      "granularity": "month",
    },
    cluster_by=['id_cnes_unidade_solicitante', 'id_cnes_unidade_executante', 'id_procedimento_sisreg'],
    on_schema_change='sync_all_columns'
  )
}}

with
    solicitacoes as (
        select 
            -- Metadados de carga/partição
            run_id as id_extracao,
            data_extracao,

            -- Identificadores da solicitacao 
            solicitacao_id as id_solicitacao,
            cast(NULL as string) as id_marcacao,

            -- Datas e timestamps do fluxo
            data_solicitacao,
            data_desejada,        
            cast(NULL as timestamp) as data_autorizacao,
            cast(NULL as timestamp) as data_execucao,
            cast(NULL as timestamp) as data_confirmacao,
            data_atualizacao as data_atualizacao_registro,        
            data_cancelamento,

            -- Status / situação e sinalizadores
            solicitacao_status,
            solicitacao_situacao,
            solicitacao_visualizada_regulador as solicitacao_visualizada_indicador,
            cast(NULL as string) as solicitacao_executada_indicador,
            cast(NULL as string) as falta_registrada_indicador,
            cast(NULL as string) as paciente_avisado_indicador,
            cast(NULL as string) as vaga_solicitada_tp,
            cast(NULL as string) as vaga_consumida_tp,
            perfil_cancelamento as cancelamento_autor,
            cast(NULL as string) as justificativa_cancelamento,
            solicitacao_risco,

            -- CID
            cid_id as cid_solicitacao,
            cast(NULL as string) as cid_marcacao,

            -- Procedimento 
            procedimento_grupo_id as id_procedimento_grupo,
            upper(trim(procedimento_grupo)) as procedimento_grupo,
            procedimento_id as id_procedimento_sisreg,
            upper(trim(procedimento)) as procedimento,
            procedimento_sigtap_id as id_procedimento_sigtap,

            -- Origem da solicitação (solicitante)
            uf_solicitante_id as id_uf_solicitante,
            cast(NULL as string) as uf_solicitante,
            central_solicitante_id_cnes as id_cnes_central_solicitante,
            central_solicitante_id as id_central_solicitante,
            coalesce(central_solicitante, central_solicitante_cnes) as central_solicitante,
            unidade_solicitante_id as id_cnes_unidade_solicitante,

            -- Profissionais/operadores da solicitação
            profissional_solicitante_cpf,
            medico_solicitante as profissional_solicitante_nome,
            crm as profissional_solicitante_crm,
            operador_solicitante_nome,
            operador_cancelamento_nome,

            -- Regulação / Centro regulador
            uf_regulador_id as id_uf_reguladora,
            cast(NULL as string) as uf_reguladora,
            central_reguladora_id as id_central_reguladora,
            central_reguladora,

            -- Unidade desejada
            unidade_desejada_id as id_cnes_unidade_desejada,

            -- Paciente (identificação e demografia)
            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_dt_nasc as paciente_data_nascimento,
            paciente_sexo,
            paciente_nome_mae,
            paciente_telefone,
            paciente_uf_nasc as paciente_uf_nascimento,
            paciente_mun_nasc as paciente_municipio_nascimento,
            paciente_uf_res as paciente_uf_residencia,
            paciente_mun_res as paciente_municipio_residencia,
            paciente_bairro_res as paciente_bairro_residencia,
            paciente_cep_res as paciente_cep_residencia,
            paciente_endereco_res as paciente_endereco_residencia,
            paciente_complemento_res as paciente_complemento_residencia,
            paciente_numero_res as paciente_numero_residencia,
            paciente_tp_logradouro_res as paciente_tp_logradouro_residencia,

            -- Laudo 
            laudo_operador_cnes_id as id_cnes_laudo_operador,
            laudo_operador,
            laudo_descricao_tp, -- ver conteudo
            laudo_situacao,
            laudo_observacao,
            laudo_data_observacao as data_laudo_observacao,

            -- Autorização do agendamento
            cast(NULL as string) as operador_autorizador_nome,

            -- Marcação
            cast(NULL as string) as id_cnes_central_executante,
            cast(NULL as string) as central_executante,
            cast(NULL as string) as id_cnes_unidade_executante,
            cast(NULL as string) as unidade_executante,            
            cast(NULL as string) as profissional_executante_cpf,
            cast(NULL as string) as profissional_executante_nome
    from {{ ref("raw_sisreg_api__solicitacoes") }}
    where 
        1=1
        {% if is_incremental() %}
            and particao_data = (select max(particao_data) from {{ this }})
        {% endif %}
    ),

    marcacoes as (
        select 
            -- Metadados de carga/partição
            run_id as id_extracao,
            data_extracao,

            -- Identificadores da solicitacao 
            solicitacao_id as id_solicitacao,
            cast(marcacao_id as string) as id_marcacao,

            -- Datas e timestamps do fluxo
            data_solicitacao,
            data_desejada,        
            data_aprovacao as data_autorizacao,
            data_marcacao as data_execucao,
            data_confirmacao,
            data_atualizacao as data_atualizacao_registro,        
            data_cancelamento,

            -- Status / situação e sinalizadores
            solicitacao_status,
            solicitacao_situacao,
            solicitacao_visualizada_regulador as solicitacao_visualizada_indicador,
            marcacao_executada as solicitacao_executada_indicador,
            falta_registrada as falta_registrada_indicador,
            paciente_avisado as paciente_avisado_indicador,
            vaga_solicitada_tp,
            vaga_consumida_tp,
            perfil_cancelamento as cancelamento_autor,
            justificativa_cancelamento,
            solicitacao_risco,

            -- CID
            cid_id as cid_solicitacao,
            cid_agendado_id as cid_marcacao,

            -- Procedimento 
            procedimento_grupo_id as id_procedimento_grupo,
            upper(trim(procedimento_grupo)) as procedimento_grupo,
            procedimento_interno_id as id_procedimento_sisreg,
            upper(trim(procedimento_interno)) as procedimento,
            procedimento_sigtap_id as id_procedimento_sigtap,

            -- Origem da solicitação (solicitante)
            uf_solicitante_id as id_uf_solicitante,
            uf_solicitante,
            central_solicitante_id_cnes as id_cnes_central_solicitante,
            central_solicitante_id as id_central_solicitante,
            coalesce(central_solicitante, central_solicitante_cnes) as central_solicitante,
            unidade_solicitante_id as id_cnes_unidade_solicitante,

            -- Profissionais/operadores da solicitação
            profissional_solicitante_cpf,
            medico_solicitante as profissional_solicitante_nome,
            crm_solicitante as profissional_solicitante_crm,
            operador_solicitante_nome,
            operador_cancelamento_nome,

            -- Regulação / Centro regulador
            uf_regulador_id as id_uf_reguladora,
            uf_regulador as uf_reguladora,
            central_reguladora_id as id_central_reguladora,
            central_reguladora,

            -- Unidade desejada
            unidade_desejada_id as id_cnes_unidade_desejada,

            -- Paciente (identificação e demografia)
            paciente_cpf,
            paciente_cns,
            paciente_nome,
            paciente_dt_nasc as paciente_data_nascimento,
            paciente_sexo,
            paciente_nome_mae,
            paciente_telefone,
            paciente_uf_nasc as paciente_uf_nascimento,
            paciente_mun_nasc as paciente_municipio_nascimento,
            paciente_uf_res as paciente_uf_residencia,
            paciente_mun_res as paciente_municipio_residencia,
            paciente_bairro_res as paciente_bairro_residencia,
            paciente_cep_res as paciente_cep_residencia,
            paciente_endereco_res as paciente_endereco_residencia,
            paciente_complemento_res as paciente_complemento_residencia,
            paciente_numero_res as paciente_numero_residencia,
            paciente_tp_logradouro_res as paciente_tp_logradouro_residencia,

            -- Laudo 
            laudo_operador_cnes_id as id_cnes_laudo_operador,
            cast(NULL as string) as laudo_operador,
            laudo_descricao_tp, -- ver conteudo
            laudo_situacao,
            laudo_observacao,
            laudo_data_observacao as data_laudo_observacao,

            -- Autorização do agendamento
            operador_autorizador_nome,

            -- Marcação
            central_executante_id_cnes as id_cnes_central_executante,
            central_executante_cnes as central_executante,
            unidade_executante_id as id_cnes_unidade_executante,
            unidade_executante_nome as unidade_executante,
            profissional_executante_cpf as profissional_executante_cpf,
            profissional_executante_nome as profissional_executante_nome
    from {{ ref("raw_sisreg_api__marcacoes") }}
    where 
        1=1
        {% if is_incremental() %}
            and particao_data = (select max(particao_data) from {{ this }})
        {% endif %}
    ),

    consolidados as (
        select * from solicitacoes
        union all
        select * from marcacoes
    ),

    enriquecimento as (
        select
            consolidados.*,
            hci.contato as paciente_contato_hci,
            hci.cns as paciente_cns_hci
        from consolidados
        left join {{ref("mart_historico_clinico__paciente")}} as hci 
        on consolidados.paciente_cpf = hci.cpf 
    ),

    final as (
        select 
            -- Metadados de carga/partição
            id_extracao,
            data_extracao,

            -- Identificadores da solicitacao 
            id_solicitacao,
            id_marcacao,

            -- Datas e timestamps do fluxo
            data_solicitacao,
            data_desejada,        
            data_autorizacao,
            data_execucao,
            data_confirmacao,
            data_atualizacao_registro,        
            data_cancelamento,

            -- Status / situação e sinalizadores
            solicitacao_status,
            solicitacao_situacao,
            solicitacao_visualizada_indicador,
            solicitacao_executada_indicador,
            falta_registrada_indicador,
            paciente_avisado_indicador,
            vaga_solicitada_tp,
            vaga_consumida_tp,
            cancelamento_autor,
            justificativa_cancelamento,
            solicitacao_risco,

            -- CID
            cid_solicitacao,
            cid_marcacao,

            -- Procedimento 
            id_procedimento_grupo,
            procedimento_grupo,
            id_procedimento_sisreg,
            procedimento, -- só marcacoes (fazer equivalente para solicitacoes)
            id_procedimento_sigtap,

            -- Origem da solicitação (solicitante)
            id_uf_solicitante,
            uf_solicitante,
            id_cnes_central_solicitante,
            id_central_solicitante,
            central_solicitante,
            id_cnes_unidade_solicitante,

            -- Profissionais/operadores da solicitação
            profissional_solicitante_cpf,
            profissional_solicitante_nome,
            profissional_solicitante_crm,
            operador_solicitante_nome,
            operador_cancelamento_nome,

            -- Regulação / Centro regulador
            id_uf_reguladora,
            uf_reguladora,
            id_central_reguladora,
            central_reguladora,

            -- Unidade desejada
            id_cnes_unidade_desejada,

            -- Paciente (identificação e demografia)
            paciente_cpf,
            paciente_cns,
            paciente_cns_hci,
            paciente_nome,
            paciente_data_nascimento,
            paciente_sexo,
            paciente_nome_mae,
            paciente_telefone,
            paciente_contato_hci,
            paciente_uf_nascimento,
            paciente_municipio_nascimento,
            paciente_uf_residencia,
            paciente_municipio_residencia,
            paciente_bairro_residencia,
            paciente_cep_residencia,
            paciente_endereco_residencia,
            paciente_complemento_residencia,
            paciente_numero_residencia,
            paciente_tp_logradouro_residencia,

            -- Laudo 
            id_cnes_laudo_operador,
            laudo_operador,
            laudo_descricao_tp, -- ver conteudo
            laudo_situacao,
            laudo_observacao,
            data_laudo_observacao,

            -- Autorização do agendamento
            operador_autorizador_nome,

            -- Marcação
            id_cnes_central_executante,
            central_executante,
            id_cnes_unidade_executante,
            unidade_executante,
            profissional_executante_cpf,
            profissional_executante_nome
        from enriquecimento
    )

select *
from final
qualify row_number() over (
    partition by id_solicitacao
    order by data_atualizacao_registro desc) = 1
