{{
    config(
        enabled=true,
        alias="visitas_acs_gestacao",
    )
}}


WITH 

marcadores_temporais AS (
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
   clinica_nome AS unidade_APS_PN,
   equipe_nome AS equipe_PN_APS
 FROM {{ ref('mart_bi_gestacoes__gestacoes') }}
),


visitas_com_join AS (
 SELECT
   ea.id_hci,
   ea.paciente.id_paciente,
   ea.entrada_data,
   ea.estabelecimento.nome AS nome_estabelecimento,
   ea.profissional_saude_responsavel.nome AS nome_profissional,
   mt.id_gestacao,
   mt.cpf,
   mt.nome_gestante,
   mt.numero_gestacao,
   mt.idade_gestante,
   mt.data_inicio,
   mt.data_fim,
   mt.data_fim_efetiva
 FROM {{ ref('mart_historico_clinico__episodio') }} ea
 JOIN marcadores_temporais mt
   ON ea.paciente.id_paciente = mt.id_paciente
   AND ea.entrada_data BETWEEN mt.data_inicio AND mt.data_fim_efetiva
 WHERE ea.prontuario.fornecedor = 'vitacare'
   AND ea.profissional_saude_responsavel.especialidade = 'Agente comunitário de saúde'
   AND ea.subtipo = 'Visita Domiciliar'
)

SELECT *, ROW_NUMBER() OVER (
        PARTITION BY
            id_gestacao
        ORDER BY entrada_data
    ) AS numero_visita
FROM visitas_com_join
ORDER BY id_gestacao, entrada_data