
{{
    config(
        enabled=true,
        alias="encaminhamentos",
    )
}}


WITH
-- CTE 1: eventos_brutos
-- Seleciona eventos clínicos brutos da tabela episodio_assistencial que podem indicar o início ou fim de uma gestação.
-- Filtra por CIDs específicos (Z32.1 - Gravidez confirmada, Z34 - Supervisão de gravidez normal, Z35 - Supervisão de gravidez de alto risco).
-- Converte a data_diagnostico para o formato DATE.
-- Classifica o tipo_evento como 'gestacao' para os CIDs relevantes.
eventos_brutos AS (
 SELECT
   paciente.id_paciente AS id_paciente,
   c.id AS cid,
   c.situacao AS situacao_cid,
   SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(c.data_diagnostico, 1, 10)) AS data_evento,
   -- Identifica o tipo de evento como 'gestacao' com base no CID
   CASE
     WHEN c.id = 'Z321' OR c.id LIKE 'Z34%' OR c.id LIKE 'Z35%' THEN 'gestacao'
     ELSE NULL -- Em teoria, o filtro WHERE já garante que só teremos 'gestacao', mas é bom ser explícito
   END AS tipo_evento
 FROM
   {{ ref('mart_historico_clinico__episodio') }},
   UNNEST(condicoes) c -- Expande o array de condições para processar cada uma individualmente
 WHERE
   c.data_diagnostico IS NOT NULL AND c.data_diagnostico != '' -- Garante que a data do diagnóstico existe
   AND c.situacao IN ('ATIVO', 'RESOLVIDO') -- Considera apenas condições ativas ou resolvidas
   AND (c.id = 'Z321' OR c.id LIKE 'Z34%' OR c.id LIKE 'Z35%') -- Filtro principal para CIDs de gestação
   AND paciente.id_paciente IS NOT NULL -- Garante que há um ID de paciente associado
),


-- CTE 2: inicios_brutos
-- Filtra 'eventos_brutos' para obter apenas os eventos que marcam o início de uma gestação (tipo 'gestacao' e situação 'ATIVO').
inicios_brutos AS (
 SELECT *
 FROM eventos_brutos
 WHERE tipo_evento = 'gestacao' AND situacao_cid = 'ATIVO'
),


-- CTE 3: finais
-- Filtra 'eventos_brutos' para obter apenas os eventos que marcam o fim de uma gestação (tipo 'gestacao' e situação 'RESOLVIDO').
finais AS (
 SELECT *
 FROM eventos_brutos
 WHERE tipo_evento = 'gestacao' AND situacao_cid = 'RESOLVIDO'
),


-- CTE 4: inicios_com_grupo
-- Prepara os dados de 'inicios_brutos' para agrupar eventos de início de gestação próximos.
-- Usa a função LAG para acessar a data do evento anterior da mesma paciente.
-- Define um 'nova_ocorrencia_flag': 1 se for o primeiro evento da paciente ou se houver um intervalo de >= 30 dias desde o evento anterior.
inicios_com_grupo AS (
 SELECT
   *,
   LAG(data_evento) OVER (PARTITION BY id_paciente ORDER BY data_evento) AS data_anterior,
   CASE
     WHEN LAG(data_evento) OVER (PARTITION BY id_paciente ORDER BY data_evento) IS NULL THEN 1 -- Primeiro evento da paciente
     WHEN DATE_DIFF(data_evento, LAG(data_evento) OVER (PARTITION BY id_paciente ORDER BY data_evento), DAY) >= 30 THEN 1 -- Nova gestação se > 30 dias
     ELSE 0 -- Continuação da mesma "janela" de início de gestação
   END AS nova_ocorrencia_flag
 FROM inicios_brutos
),


-- CTE 5: grupos_inicios
-- Cria um 'grupo_id' para cada conjunto de eventos de início de gestação que pertencem à mesma gestação.
-- Usa a soma acumulada (SUM OVER) do 'nova_ocorrencia_flag'.
grupos_inicios AS (
 SELECT
   *,
   SUM(nova_ocorrencia_flag) OVER (PARTITION BY id_paciente ORDER BY data_evento) AS grupo_id
 FROM inicios_com_grupo
),


-- CTE 6: inicios_deduplicados
-- Seleciona o evento de início mais recente dentro de cada 'grupo_id' para uma paciente.
-- Isso ajuda a consolidar múltiplos registros de início próximos no tempo para uma única data de início.
-- A ordenação por `data_evento DESC` e `rn = 1` pega o registro mais recente dentro do grupo.
-- Se a intenção fosse pegar o primeiro registro do grupo, seria `ASC`. A lógica atual pega o último sinal de "ativo" dentro do grupo.
inicios_deduplicados AS (
 SELECT *
 FROM (
   SELECT
     *,
     ROW_NUMBER() OVER (PARTITION BY id_paciente, grupo_id ORDER BY data_evento DESC) AS rn
   FROM grupos_inicios
 )
 WHERE rn = 1
),


-- CTE 7: gestacoes_unicas
-- Define cada gestação única com sua data de início e data de fim.
-- A data de fim é o primeiro evento 'RESOLVIDO' (de 'finais') que ocorre após a data de início da gestação.
-- Gera um 'numero_gestacao' e um 'id_gestacao' único para cada gestação da paciente.
gestacoes_unicas AS (
 SELECT
   i.id_paciente,
   i.data_evento AS data_inicio,
   -- Subconsulta para encontrar a data de fim mais próxima após o início
   (
     SELECT MIN(f.data_evento)
     FROM finais f
     WHERE f.id_paciente = i.id_paciente AND f.data_evento > i.data_evento
   ) AS data_fim,
   ROW_NUMBER() OVER (PARTITION BY i.id_paciente ORDER BY i.data_evento) AS numero_gestacao,
   CONCAT(i.id_paciente, '-', CAST(ROW_NUMBER() OVER (PARTITION BY i.id_paciente ORDER BY i.data_evento) AS STRING)) AS id_gestacao
 FROM inicios_deduplicados i
),


-- CTE 8: gestacoes_com_status
-- Calcula a 'data_fim_efetiva' (data de fim real ou estimada após 308 dias se não houver fim registrado e já passou o período).
-- Calcula a DPP (Data Provável do Parto) como 40 semanas após a data de início.
gestacoes_com_status AS (
 SELECT
   *,
   CASE
     WHEN data_fim IS NOT NULL THEN data_fim -- Usa a data de fim registrada se existir
     WHEN DATE_ADD(data_inicio, INTERVAL 308 DAY) <= CURRENT_DATE() THEN DATE_ADD(data_inicio, INTERVAL 308 DAY) -- Se passou 308 dias e não tem data_fim, considera encerrada
     ELSE NULL -- Ainda em curso e sem data de fim, ou não atingiu 308 dias. Será tratada pela fase_atual.
   END AS data_fim_efetiva,
   DATE_ADD(data_inicio, INTERVAL 40 WEEK) AS dpp
 FROM gestacoes_unicas
),


-- CTE 9: filtrado (anteriormente chamado gestacoes_com_fase)
-- Determina a 'fase_atual' da gestação (Gestação, Puerpério, Encerrada).
-- Esta CTE é central e será usada para juntar a maioria das outras informações.
filtrado AS (
 SELECT
   gcs.*,
   CASE
     WHEN gcs.data_fim IS NULL AND DATE_ADD(gcs.data_inicio, INTERVAL 308 DAY) > CURRENT_DATE() THEN 'Gestação' -- Sem data de fim e dentro dos 308 dias esperados
     WHEN gcs.data_fim IS NOT NULL AND DATE_DIFF(CURRENT_DATE(), gcs.data_fim, DAY) <= 45 THEN 'Puerpério' -- Com data de fim, e dentro de 45 dias do fim
     ELSE 'Encerrada' -- Outros casos (com data de fim e > 45 dias, ou sem data de fim mas > 308 dias)
   END AS fase_atual,
   -- Adicionando o cálculo do trimestre aqui para evitar uma CTE separada depois
   CASE
     WHEN DATE_DIFF(CURRENT_DATE(), gcs.data_inicio, WEEK) <= 13 THEN '1º trimestre'
     WHEN DATE_DIFF(CURRENT_DATE(), gcs.data_inicio, WEEK) BETWEEN 14 AND 27 THEN '2º trimestre'
     WHEN DATE_DIFF(CURRENT_DATE(), gcs.data_inicio, WEEK) >= 28 THEN '3º trimestre'
     ELSE 'Data inválida ou encerrada' -- Pode precisar ajustar essa lógica se `fase_atual` != 'Gestação'
   END AS trimestre_atual_gestacao -- Renomeado para clareza
 FROM gestacoes_com_status gcs
),


pacientes_info AS (
 SELECT
   p_dedup.dados.id_paciente,
   p_dedup.cpf,
   p_dedup.cns,
   p_dedup.dados.nome,
   p_dedup.dados.data_nascimento,
   p_dedup.`equipe_saude_familia`[SAFE_OFFSET(0)].clinica_familia.id_cnes,
   DATE_DIFF(CURRENT_DATE(), p_dedup.dados.data_nascimento, YEAR) AS idade_atual, -- Idade atual do paciente
   CASE
     WHEN DATE_DIFF(CURRENT_DATE(), p_dedup.dados.data_nascimento, YEAR) <= 15 THEN '≤15 anos'
     WHEN DATE_DIFF(CURRENT_DATE(), p_dedup.dados.data_nascimento, YEAR) <= 20 THEN '16-20 anos'
     WHEN DATE_DIFF(CURRENT_DATE(), p_dedup.dados.data_nascimento, YEAR) <= 30 THEN '21-30 anos'
     WHEN DATE_DIFF(CURRENT_DATE(), p_dedup.dados.data_nascimento, YEAR) <= 40 THEN '31-40 anos'
     ELSE '>40 anos'
   END AS faixa_etaria,
   p_dedup.dados.raca,
   p_dedup.dados.obito_indicador,
   p_dedup.dados.obito_data
 FROM (
   SELECT *,
          ROW_NUMBER() OVER (PARTITION BY dados.id_paciente ORDER BY cpf_particao DESC) AS rn
   FROM {{ ref('mart_historico_clinico__paciente') }}
 ) p_dedup
 WHERE p_dedup.rn = 1
),

encaminhamento_SISREG AS (
 SELECT
   f.id_gestacao,
   pi.id_paciente,
   s.paciente_cpf,
   s.paciente_cns,
   s.paciente_nome,
   s.paciente_dt_nasc,
   s.data_solicitacao,
   DATE(s.data_solicitacao) AS data_solicitacao_date,
   s.solicitacao_status,
   s.solicitacao_situacao,
   s.procedimento,
   s.procedimento_id,
   s.unidade_solicitante,
   s.medico_solicitante,
   s.operador_solicitante_nome,
   ROW_NUMBER() OVER (
     PARTITION BY f.id_gestacao
     ORDER BY
       s.data_solicitacao ASC,
       s.procedimento_id ASC
   ) as rn_solicitacao
 FROM filtrado f -- Certifique-se que 'filtrado' é uma tabela ou CTE existente
 JOIN pacientes_info pi ON f.id_paciente = pi.id_paciente
 LEFT JOIN
   {{ ref('raw_sisreg_api__solicitacoes') }} s
   ON
     (
       ((pi.cpf IS NOT NULL AND pi.cpf != '') AND (s.paciente_cpf IS NOT NULL AND s.paciente_cpf != '') AND pi.cpf = s.paciente_cpf)
       -- OR
       -- ((pi.cns IS NOT NULL AND ARRAY_LENGTH(pi.cns) > 0) AND (s.paciente_cns IS NOT NULL AND s.paciente_cns != '') AND s.paciente_cns IN UNNEST(pi.cns))
     )
     AND s.procedimento_id IN ('0703844','0703886','0737024','0710301','0710128')
     AND DATE(s.data_solicitacao) BETWEEN f.data_inicio AND COALESCE(f.data_fim_efetiva, CURRENT_DATE())
)




-- Consulta principal que utiliza a CTE
SELECT
 f.id_gestacao, -- Adicionei f.id_gestacao e outras colunas de 'f' que você possa precisar
 f.id_paciente, -- Exemplo
 -- f.outra_coluna_de_filtrado, -- Adicione outras colunas de 'f' conforme necessário
 sis_sol.data_solicitacao_date AS sisreg_primeira_data_solicitacao_data,
 sis_sol.solicitacao_status AS sisreg_primeira_status,
 sis_sol.solicitacao_situacao AS sisreg_primeira_situacao,
 sis_sol.procedimento AS sisreg_primeira_procedimento_nome,
 sis_sol.procedimento_id AS sisreg_primeira_procedimento_id,
 sis_sol.unidade_solicitante AS sisreg_primeira_unidade_solicitante,
 sis_sol.medico_solicitante AS sisreg_primeira_medico_solicitante,
 sis_sol.operador_solicitante_nome AS sisreg_primeira_operador_solicitante
FROM
 filtrado f -- Certifique-se que 'filtrado' é uma tabela ou CTE existente
LEFT JOIN
 (SELECT * FROM encaminhamento_SISREG WHERE rn_solicitacao = 1) sis_sol
 ON f.id_gestacao = sis_sol.id_gestacao
WHERE f.fase_atual = 'Gestação' -- Filtro aplicado no final