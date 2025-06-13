{{
    config(
        enabled=true,
        alias="consultas_emergenciais",
    )
}}


WITH marcadores_temporais AS (
 SELECT
   id_gestacao,
   id_paciente,
   cpf,
   nome AS nome_gestante,
   numero_gestacao,
   idade_gestante,
   data_inicio,
   data_fim,
   data_fim_efetiva,
   fase_atual,
   clinica_nome AS unidade_APS_PN,
   equipe_nome AS equipe_PN_APS
 FROM {{ ref('mart_bi_gestacoes__gestacoes_com_fase') }}
),


cids_agrupados AS (
 SELECT
   ea.id_hci,
   STRING_AGG(DISTINCT c.id ORDER BY c.id) AS cids_emergencia,
   STRING_AGG(DISTINCT c.descricao ORDER BY c.descricao) AS descricoes_cids_emergencia
 FROM {{ ref('mart_historico_clinico__episodio') }} ea,
      UNNEST(ea.condicoes) AS c
 WHERE ea.prontuario.fornecedor = 'vitai'
   AND ea.subtipo = 'Emergência'
   AND c.id IS NOT NULL
 GROUP BY ea.id_hci
),


atendimentos_ue_com_join AS (
 SELECT
   ea.id_hci,
   ea.paciente.id_paciente,
   ea.entrada_data AS data_consulta,
   ea.estabelecimento.nome AS nome_estabelecimento,
   ea.profissional_saude_responsavel.nome AS nome_profissional,
   ea.motivo_atendimento,
   ea.desfecho_atendimento,
   mt.id_gestacao,
   mt.cpf,
   mt.nome_gestante,
   mt.numero_gestacao,
   mt.idade_gestante,
   mt.data_inicio,
   mt.data_fim,
   mt.data_fim_efetiva,
   cids.cids_emergencia
 FROM {{ ref('mart_historico_clinico__episodio') }} ea
 JOIN marcadores_temporais mt
 ON ea.paciente.id_paciente = mt.id_paciente
 AND ea.entrada_data BETWEEN mt.data_inicio AND COALESCE(mt.data_fim_efetiva, CURRENT_DATE())


 LEFT JOIN cids_agrupados cids ON ea.id_hci = cids.id_hci
 WHERE ea.prontuario.fornecedor = 'vitai'
   AND ea.subtipo = 'Emergência'
)

SELECT
    -- Identificação da gestação e paciente
    mt.id_gestacao, mt.numero_gestacao, mt.id_paciente, mt.cpf, mt.nome_gestante, mt.idade_gestante,

-- Dados da gestação
mt.data_inicio,
mt.data_fim,
mt.data_fim_efetiva,
mt.unidade_APS_PN,
mt.equipe_PN_APS,
mt.fase_atual,

-- Atendimento de emergência
ea.id_hci,
ea.entrada_data AS data_consulta,
DATE_DIFF (
    ea.entrada_data,
    mt.data_inicio,
    WEEK
) AS idade_gestacional_consulta,
ROW_NUMBER() OVER (
    PARTITION BY
        mt.id_gestacao
    ORDER BY ea.entrada_data
) AS numero_consulta,
ea.motivo_atendimento,
ea.desfecho_atendimento,
cids.cids_emergencia,
cids.descricoes_cids_emergencia,

-- Profissional e estabelecimento
ea.profissional_saude_responsavel.nome AS nome_profissional,
 ea.profissional_saude_responsavel.especialidade AS especialidade_profissional,
 ea.estabelecimento.nome AS nome_estabelecimento


FROM {{ ref('mart_historico_clinico__episodio') }} ea
JOIN marcadores_temporais mt
 ON ea.paciente.id_paciente = mt.id_paciente
 AND ea.entrada_data BETWEEN mt.data_inicio AND COALESCE(mt.data_fim_efetiva, CURRENT_DATE())


LEFT JOIN cids_agrupados cids
 ON ea.id_hci = cids.id_hci
WHERE ea.prontuario.fornecedor = 'vitai'
 AND ea.subtipo = 'Emergência'
ORDER BY mt.id_gestacao, ea.entrada_data