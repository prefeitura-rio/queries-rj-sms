with

sisreg_marcacoes as (
    select
        -- profissionais (hashear todos)
        profissional_solicitante_cpf,
        profissional_executante_cpf,
        operador_solicitante_nome,
        operador_autorizador_nome,
        operador_cancelamento_nome,
        operador_videofonista_nome,

        -- unidades
        central_solicitante,
        central_reguladora,
        unidade_solicitante_id as unidade_solicitante_id_cnes,
        unidade_desejada_id as unidade_desejada_id_cnes,
        unidade_executante_id as unidade_executante_id_cnes,

        -- paciente
        paciente_cpf, -- hashear
        paciente_dt_nasc, -- transformar em faixa etaria
        paciente_sexo,

        -- solicitacao & andamento
        solicitacao_id, -- hashear
        data_solicitacao,
        data_desejada,
        data_aprovacao,
        data_confirmacao,
        data_marcacao,
        data_cancelamento,
        data_atualizacao,
        solicitacao_status,
        solicitacao_situacao,
        solicitacao_visualizada_regulador,
        solicitacao_risco,
        paciente_avisado,
        marcacao_executada,
        falta_registrada,

        -- procedimento & cid
        procedimento_interno_id as procedimento_sisreg_id,
        procedimento_grupo,
        vaga_solicitada_tp,
        vaga_consumida_tp,
        cid_id as cid_solicitado_id,
        cid_agendado_id,

        -- laudo
        laudo_descricao_tp,
        laudo_situacao,
        laudo_observacao,
        laudo_data_observacao

    from {{ ref("raw_sisreg_api__marcacoes") }}
    where 1 = 1
        and uf_solicitante_id = "33"
        and uf_regulador_id = "33"
        and paciente_uf_res = "RJ"
        and paciente_mun_res = "RIO DE JANEIRO"
        and data_solicitacao >= '2022-01-01' 
        and data_solicitacao < '2025-01-01'
)   

select *
from sisreg_marcacoes
