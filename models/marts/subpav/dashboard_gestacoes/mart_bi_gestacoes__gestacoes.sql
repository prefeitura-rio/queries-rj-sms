{{
    config(
        enabled=true,
        alias="gestacoes",
    )
}}

WITH

    -- ------------------------------------------------------------
    -- Recuperando dados do Paciente
    -- ------------------------------------------------------------
    cadastro_paciente AS (
        SELECT
            dados.id_paciente,
            dados.nome,
            DATE_DIFF (
                CURRENT_DATE(),
                dados.data_nascimento,
                YEAR
            ) AS idade_gestante,
        FROM {{ ref('mart_historico_clinico__paciente') }}       
    ),

    -- ------------------------------------------------------------
    -- Eventos de Gestação
    -- ------------------------------------------------------------
    -- Seleciona eventos clínicos brutos da tabela episodio_assistencial que podem indicar o início ou fim de uma gestação.
    -- Filtra por CIDs específicos (Z32.1 - Gravidez confirmada, Z34 - Supervisão de gravidez normal, Z35 - Supervisão de gravidez de alto risco).
    -- Classifica o tipo_evento como 'gestacao' para os CIDs relevantes.
    -- ------------------------------------------------------------
    eventos_brutos AS (
        SELECT
            id_hci,
            paciente.id_paciente AS id_paciente,
            paciente_cpf as cpf,
            cp.nome,
            cp.idade_gestante,
            c.id AS cid,
            c.situacao AS situacao_cid,
            SAFE.PARSE_DATE (
                '%Y-%m-%d',
                SUBSTR(c.data_diagnostico, 1, 10)
            ) AS data_evento,
            -- Identifica o tipo de evento como 'gestacao' com base no CID
            CASE
                WHEN c.id = 'Z321'
                OR c.id LIKE 'Z34%'
                OR c.id LIKE 'Z35%' THEN 'gestacao'
                ELSE NULL -- Em teoria, o filtro WHERE já garante que só teremos 'gestacao', mas é bom ser explícito
            END AS tipo_evento
        FROM
            {{ ref('mart_historico_clinico__episodio') }}
            --Ajuste UNNEST (foi retirado a vírgula ao fim da linha acima)
            LEFT JOIN UNNEST (condicoes) c -- Expande o array de condições para processar cada uma individualmente
            INNER JOIN cadastro_paciente cp ON paciente.id_paciente = cp.id_paciente
        WHERE
            c.data_diagnostico IS NOT NULL
            AND c.data_diagnostico != '' -- Garante que a data do diagnóstico existe
            AND c.situacao IN ('ATIVO', 'RESOLVIDO') -- Considera apenas condições ativas ou resolvidas
            AND (
                c.id = 'Z321'
                OR c.id LIKE 'Z34%'
                OR c.id LIKE 'Z35%'
            ) -- Filtro principal para CIDs de gestação
            AND paciente.id_paciente IS NOT NULL -- Garante que há um ID de paciente associado
    ),

    -- ------------------------------------------------------------
    -- Inícios e Finais de Gestação
    -- ------------------------------------------------------------
    inicios_brutos AS (
        SELECT *
        FROM eventos_brutos
        WHERE
            tipo_evento = 'gestacao'
            AND situacao_cid = 'ATIVO'
    ),
    finais AS (
        SELECT *
        FROM eventos_brutos
        WHERE
            tipo_evento = 'gestacao'
            AND situacao_cid = 'RESOLVIDO'
    ),

    -- ------------------------------------------------------------
    -- Inícios de Gestação com Grupo
    -- ------------------------------------------------------------
    -- Prepara os dados de 'inicios_brutos' para agrupar eventos de início de gestação próximos.
    -- Usa a função LAG para acessar a data do evento anterior da mesma paciente.
    -- Define um 'nova_ocorrencia_flag': 1 se for o primeiro evento da paciente ou se houver um intervalo de >= 30 dias desde o evento anterior.
    -- ------------------------------------------------------------
    inicios_com_grupo AS (
        SELECT
            *,
            LAG(data_evento) OVER (
                PARTITION BY
                    id_paciente
                ORDER BY data_evento
            ) AS data_anterior,
            CASE
                WHEN LAG(data_evento) OVER (
                    PARTITION BY
                        id_paciente
                    ORDER BY data_evento
                ) IS NULL THEN 1 -- Primeiro evento da paciente
                WHEN DATE_DIFF (
                    data_evento,
                    LAG(data_evento) OVER (
                        PARTITION BY
                            id_paciente
                        ORDER BY data_evento
                    ),
                    DAY
                ) >= 30 THEN 1 -- Nova gestação se > 30 dias
                ELSE 0 -- Continuação da mesma "janela" de início de gestação
            END AS nova_ocorrencia_flag
        FROM inicios_brutos
    ),

    -- ------------------------------------------------------------
    -- Grupos de Inícios de Gestação
    -- ------------------------------------------------------------
    -- Cria um 'grupo_id' para cada conjunto de eventos de início de gestação que pertencem à mesma gestação.
    -- Usa a soma acumulada (SUM OVER) do 'nova_ocorrencia_flag'.
    -- ------------------------------------------------------------
    grupos_inicios AS (
        SELECT *, SUM(nova_ocorrencia_flag) OVER (
                PARTITION BY
                    id_paciente
                ORDER BY data_evento
            ) AS grupo_id
        FROM inicios_com_grupo
    ),

    -- ------------------------------------------------------------
    -- Inícios de Gestação Deduplicados
    -- ------------------------------------------------------------
    -- Seleciona o evento de início mais recente dentro de cada 'grupo_id' para uma paciente.
    -- Isso ajuda a consolidar múltiplos registros de início próximos no tempo para uma única data de início.
    -- A ordenação por `data_evento DESC` e `rn = 1` pega o registro mais recente dentro do grupo.
    -- Se a intenção fosse pegar o primeiro registro do grupo, seria `ASC`. A lógica atual pega o último sinal de "ativo" dentro do grupo.
    -- ------------------------------------------------------------
    inicios_deduplicados AS (
        SELECT *
        FROM (
                SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY
                            id_paciente, grupo_id
                        ORDER BY data_evento DESC
                    ) AS rn
                FROM grupos_inicios
            )
        WHERE
            rn = 1
    ),

    -- ------------------------------------------------------------
    -- Gestações Únicas
    -- ------------------------------------------------------------
    -- Define cada gestação única com sua data de início e data de fim.
    -- A data de fim é o primeiro evento 'RESOLVIDO' (de 'finais') que ocorre após a data de início da gestação.
    -- Gera um 'numero_gestacao' e um 'id_gestacao' único para cada gestação da paciente.
    -- ------------------------------------------------------------
    gestacoes_unicas AS (
        SELECT
            i.id_hci,
            i.id_paciente,
            i.cpf,
            i.nome,
            i.idade_gestante,
            i.data_evento AS data_inicio,
            -- Subconsulta para encontrar a data de fim mais próxima após o início
            (
                SELECT MIN(f.data_evento)
                FROM finais f
                WHERE
                    f.id_paciente = i.id_paciente
                    AND f.data_evento > i.data_evento
            ) AS data_fim,
            ROW_NUMBER() OVER (
                PARTITION BY
                    i.id_paciente
                ORDER BY i.data_evento
            ) AS numero_gestacao,
            CONCAT(
                i.id_paciente,
                '-',
                CAST(
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            i.id_paciente
                        ORDER BY i.data_evento
                    ) AS STRING
                )
            ) AS id_gestacao
        FROM inicios_deduplicados i
    ),

    -- ------------------------------------------------------------
    -- Gestações com Status
    -- ------------------------------------------------------------
    -- Calcula a 'data_fim_efetiva' (data de fim real ou estimada após 300 dias se não houver fim registrado e já passou o período).
    -- Calcula a DPP (Data Provável do Parto) como 40 semanas após a data de início.
    -- ------------------------------------------------------------
    gestacoes_com_status AS (
        SELECT
            *,
            CASE
                WHEN data_fim IS NOT NULL THEN data_fim -- Usa a data de fim registrada se existir
                WHEN DATE_ADD(data_inicio, INTERVAL 300 DAY) <= CURRENT_DATE() THEN DATE_ADD(data_inicio, INTERVAL 300 DAY) -- Se passou 300 dias (42sem + 6 dias) e não tem data_fim, considera encerrada
                ELSE NULL -- Ainda em curso e sem data de fim, ou não atingiu 300 dias. Será tratada pela fase_atual.
            END AS data_fim_efetiva,
            DATE_ADD(data_inicio, INTERVAL 40 WEEK) AS dpp
        FROM gestacoes_unicas
    ),

    -- ------------------------------------------------------------
    -- Definição de Fase e Trimestre da Gestação
    -- ------------------------------------------------------------
    filtrado AS (
        SELECT
            gcs.*,
            CASE
                WHEN gcs.data_fim IS NULL
                AND DATE_ADD(
                    gcs.data_inicio,
                    INTERVAL 300 DAY
                ) > CURRENT_DATE() THEN 'Gestação' -- Sem data de fim e dentro dos 300 dias esperados
                WHEN gcs.data_fim IS NOT NULL
                AND DATE_DIFF (
                    CURRENT_DATE(),
                    gcs.data_fim,
                    DAY
                ) <= 45 THEN 'Puerpério' -- Com data de fim, e dentro de 45 dias do fim
                ELSE 'Encerrada' -- Outros casos (com data de fim e > 45 dias, ou sem data de fim mas > 300 dias)
            END AS fase_atual,
            -- Adicionando o cálculo do trimestre aqui para evitar uma CTE separada depois
            CASE
                WHEN DATE_DIFF (
                    CURRENT_DATE(),
                    gcs.data_inicio,
                    WEEK
                ) <= 13 THEN '1º trimestre'
                WHEN DATE_DIFF (
                    CURRENT_DATE(),
                    gcs.data_inicio,
                    WEEK
                ) BETWEEN 14 AND 27  THEN '2º trimestre'
                WHEN DATE_DIFF (
                    CURRENT_DATE(),
                    gcs.data_inicio,
                    WEEK
                ) >= 28 THEN '3º trimestre'
                ELSE 'Data inválida ou encerrada' -- Pode precisar ajustar essa lógica se `fase_atual` != 'Gestação'
            END AS trimestre_atual_gestacao -- Renomeado para clareza
        FROM gestacoes_com_status gcs
    ),

    -- ------------------------------------------------------------
    -- Descobrindo Equipe da Saúde da Gestação
    -- ------------------------------------------------------------
    unnested_equipes AS (
        SELECT
            p.dados.id_paciente AS id_paciente,
            eq.datahora_ultima_atualizacao,
            eq.nome AS equipe_nome,
            eq.clinica_familia.nome AS clinica_nome
        FROM
            -- {{ ref('mart_historico_clinico__paciente') }} p,
            {{ ref('mart_historico_clinico__paciente') }} p
            --Ajuste UNNEST (foi retirado a vírgula ao fim da linha acima)
            left join UNNEST (p.equipe_saude_familia) AS eq
    ),
    equipe_durante_gestacao AS (
        SELECT f.id_gestacao, -- Chave para JOIN posterior
            eq.equipe_nome, eq.clinica_nome, ROW_NUMBER() OVER (
                PARTITION BY
                    f.id_gestacao -- Modificado para id_gestacao para garantir unicidade por gestação
                ORDER BY eq.datahora_ultima_atualizacao DESC
            ) AS rn
        FROM
            filtrado f
                LEFT JOIN unnested_equipes eq ON f.id_paciente = eq.id_paciente
            -- A equipe deve ter sido atualizada ANTES ou NO MÁXIMO na data de fim da gestação
            AND DATE(
                eq.datahora_ultima_atualizacao
            ) <= COALESCE(
                f.data_fim_efetiva,
                CURRENT_DATE()
            )
    ),
    equipe_durante_final AS (
        SELECT
            id_gestacao,
            equipe_nome,
            clinica_nome
        FROM equipe_durante_gestacao
        WHERE
            rn = 1
    )

-- ------------------------------------------------------------
-- Finalização do Modelo
-- ------------------------------------------------------------
SELECT
    filtrado.id_hci,
    filtrado.id_gestacao,
    filtrado.id_paciente,
    filtrado.cpf,
    filtrado.nome,
    filtrado.idade_gestante,
    filtrado.numero_gestacao,
    filtrado.data_inicio,
    filtrado.data_fim,
    filtrado.data_fim_efetiva,
    filtrado.dpp,
    filtrado.fase_atual,
    filtrado.trimestre_atual_gestacao,
    edf.equipe_nome,
    edf.clinica_nome
FROM filtrado
    LEFT JOIN equipe_durante_final edf ON filtrado.id_gestacao = edf.id_gestacao