{{
    config(
        enabled=true,
        alias="gestacoes",
    )
}}

-- ================================================================================================
-- REGRA "JANELA DE GESTAÇÃO ABERTA" (issue #42 — corrige a duplicação de gestantes)
-- ------------------------------------------------------------------------------------------------
-- A regra anterior (agrupamento por gap de 60 dias) fragmentava uma gravidez em várias gestações
-- ATIVAS simultâneas: qualquer CID de gestação ATIVO recorrente (Z34/Z35, supervisão) ou re-datado
-- (Z321) virava um novo "início" quando distava >= 60 dias do anterior, e nada encerrava a gestação
-- por PARTO (só RESOLVIDO). Resultado: 2.054 pacientes com 2-4 gestações abertas ao mesmo tempo.
--
-- Nova regra (sessionization recursiva, uma gestação por iteração):
--   * Uma abertura só cria nova gestação se NÃO cair dentro da janela de uma gestação já aberta.
--   * Janela = [data_inicio, min(1º fechamento posterior, data_inicio + 299 dias)].
--   * Fechamentos = RESOLVIDO de CID de gestação  UNIÃO  partos/abortos maternos
--     (O00-O04, O80-O84, Z37/Z38/Z39; fornecedor vitai; por entrada_data; a partir de 2021).
--   * data_inicio = PRIMEIRO evento do grupo (corrige M4 — antes pegava o último via DESC).
--   * data_fim  = fechamento LIMITADO à janela; NULL se a janela venceu por 299 dias (corrige M3).
--   * Carência de 45 dias pós-fechamento (= janela de puerpério): eventos até 45 dias após um
--     fechamento real NÃO abrem gestação nova (evita re-stamp puerperal do Z321 criar fantasma).
--
-- Contrato de saída inalterado (15 colunas) — proced_2..8 consomem esta tabela sem mudança.
-- ================================================================================================

WITH RECURSIVE

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
    -- Eventos de Gestação (CIDs Z32.1 / Z34% / Z35%), ATIVO ou RESOLVIDO
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
            CASE
                WHEN c.id = 'Z321'
                OR c.id LIKE 'Z34%'
                OR c.id LIKE 'Z35%' THEN 'gestacao'
                ELSE NULL
            END AS tipo_evento
        FROM
            {{ ref('mart_historico_clinico__episodio') }}
            LEFT JOIN UNNEST (condicoes) c
            INNER JOIN cadastro_paciente cp ON paciente.id_paciente = cp.id_paciente
        WHERE
            c.data_diagnostico IS NOT NULL
            AND c.data_diagnostico != ''
            AND c.situacao IN ('ATIVO', 'RESOLVIDO')
            AND (
                c.id = 'Z321'
                OR c.id LIKE 'Z34%'
                OR c.id LIKE 'Z35%'
            )
            AND paciente.id_paciente IS NOT NULL
    ),

    -- ------------------------------------------------------------
    -- Inícios (ATIVO) e Finais (RESOLVIDO) de gestação
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
    -- NOVO: Partos/abortos maternos — encerram a gestação (não só o RESOLVIDO)
    -- Fornecedor 'vitai', datado por entrada_data, a partir de 2021.
    -- CIDs: O00-O04 (gravidez ectópica/aborto), O80-O84 (parto), Z37/Z38/Z39 (resultado/RN/pós-parto).
    -- ------------------------------------------------------------
    eventos_parto_mae AS (
        SELECT DISTINCT
            ea.paciente.id_paciente AS id_paciente,
            ea.entrada_data AS data_fechamento
        FROM {{ ref('mart_historico_clinico__episodio') }} ea
        LEFT JOIN UNNEST (ea.condicoes) c
        WHERE ea.entrada_data >= DATE '2021-01-01'
            AND LOWER(ea.prontuario.fornecedor) = 'vitai'
            AND (
                REGEXP_CONTAINS(c.id, r'^O0[0-4]')
                OR REGEXP_CONTAINS(c.id, r'^O8[0-4]')
                OR c.id LIKE 'Z37%'
                OR c.id LIKE 'Z38%'
                OR c.id LIKE 'Z39%'
            )
            AND ea.paciente.id_paciente IS NOT NULL
    ),

    -- ------------------------------------------------------------
    -- NOVO: Aberturas (datas ATIVO deduplicadas) e Fechamentos (RESOLVIDO ∪ partos)
    -- ------------------------------------------------------------
    aberturas AS (
        SELECT id_paciente, data_evento
        FROM inicios_brutos
        GROUP BY id_paciente, data_evento
    ),
    fechamentos AS (
        SELECT DISTINCT id_paciente, data_evento AS data_fechamento FROM finais
        UNION DISTINCT
        SELECT id_paciente, data_fechamento FROM eventos_parto_mae
    ),

    -- ------------------------------------------------------------
    -- NOVO: Estado por paciente — arrays ordenados de aberturas (da) e fechamentos (df)
    -- ------------------------------------------------------------
    estado_paciente AS (
        SELECT
            a.id_paciente,
            ARRAY_AGG(a.data_evento ORDER BY a.data_evento) AS da,
            ANY_VALUE(fx.df) AS df,
            COUNT(*) AS n_aberturas
        FROM aberturas a
        LEFT JOIN (
            SELECT id_paciente, ARRAY_AGG(data_fechamento ORDER BY data_fechamento) AS df
            FROM fechamentos
            GROUP BY id_paciente
        ) fx USING (id_paciente)
        GROUP BY a.id_paciente
    ),

    -- ------------------------------------------------------------
    -- NOVO: Sessionization recursiva — 1 gestação por iteração
    --   fim_janela = LEAST(1º fechamento > data_inicio, data_inicio + 299 dias)
    --   próxima abertura = 1ª data em `da` maior que o LIMITE DE CARÊNCIA, onde
    --   limite = fim_janela + 45 dias se a janela fechou por fechamento real;
    --           = fim_janela           se venceu por 299 dias (sem carência).
    -- ------------------------------------------------------------
    gest_rec AS (
        -- Termo âncora: 1ª gestação da paciente (1ª abertura)
        SELECT
            id_paciente,
            1 AS n,
            da[OFFSET(0)] AS data_inicio,
            LEAST(
                COALESCE((SELECT MIN(f) FROM UNNEST(df) f WHERE f > da[OFFSET(0)]), DATE '9999-12-31'),
                DATE_ADD(da[OFFSET(0)], INTERVAL 299 DAY)
            ) AS fim_janela,
            da,
            df
        FROM estado_paciente
        WHERE n_aberturas <= 200  -- guarda de segurança contra array patológico
        UNION ALL
        -- Termo recursivo: próxima gestação começa na 1ª abertura após a carência
        SELECT
            id_paciente,
            n + 1,
            (SELECT MIN(d) FROM UNNEST(da) d
               WHERE d > IF(fim_janela < DATE_ADD(data_inicio, INTERVAL 299 DAY),
                            DATE_ADD(fim_janela, INTERVAL 45 DAY), fim_janela)),
            LEAST(
                COALESCE((
                    SELECT MIN(f) FROM UNNEST(df) f
                    WHERE f > (SELECT MIN(d) FROM UNNEST(da) d
                                 WHERE d > IF(fim_janela < DATE_ADD(data_inicio, INTERVAL 299 DAY),
                                              DATE_ADD(fim_janela, INTERVAL 45 DAY), fim_janela))
                ), DATE '9999-12-31'),
                DATE_ADD((SELECT MIN(d) FROM UNNEST(da) d
                            WHERE d > IF(fim_janela < DATE_ADD(data_inicio, INTERVAL 299 DAY),
                                         DATE_ADD(fim_janela, INTERVAL 45 DAY), fim_janela)),
                         INTERVAL 299 DAY)
            ),
            da,
            df
        FROM gest_rec
        WHERE (SELECT MIN(d) FROM UNNEST(da) d
                 WHERE d > IF(fim_janela < DATE_ADD(data_inicio, INTERVAL 299 DAY),
                              DATE_ADD(fim_janela, INTERVAL 45 DAY), fim_janela)) IS NOT NULL
            AND n < 100  -- cap de profundidade (nº máx. de gestações por paciente)
    ),

    -- ------------------------------------------------------------
    -- NOVO: Evento representante por abertura (carrega id_hci/cpf/nome/idade; prefere Z321)
    -- ------------------------------------------------------------
    evento_representante AS (
        SELECT
            id_paciente,
            data_evento,
            ARRAY_AGG(
                STRUCT(id_hci, cpf, nome, idade_gestante)
                ORDER BY IF(cid = 'Z321', 0, 1), id_hci
                LIMIT 1
            )[OFFSET(0)] AS rep
        FROM inicios_brutos
        GROUP BY id_paciente, data_evento
    ),

    -- ------------------------------------------------------------
    -- Gestações Únicas — projeta a recursão + representante
    --   data_inicio = 1ª abertura do grupo (M4);  data_fim = fechamento na janela ou NULL (M3).
    -- ------------------------------------------------------------
    gestacoes_unicas AS (
        SELECT
            er.rep.id_hci AS id_hci,
            g.id_paciente,
            er.rep.cpf AS cpf,
            er.rep.nome AS nome,
            er.rep.idade_gestante AS idade_gestante,
            g.data_inicio,
            IF(g.fim_janela = DATE_ADD(g.data_inicio, INTERVAL 299 DAY), NULL, g.fim_janela) AS data_fim,
            g.n AS numero_gestacao,
            CONCAT(g.id_paciente, '-', CAST(g.n AS STRING)) AS id_gestacao
        FROM gest_rec g
        JOIN evento_representante er
            ON er.id_paciente = g.id_paciente
            AND er.data_evento = g.data_inicio
    ),

    -- ------------------------------------------------------------
    -- Gestações com Status — data_fim_efetiva (fim real ou +299d) e DPP (inalterado)
    -- ------------------------------------------------------------
    gestacoes_com_status AS (
        SELECT
            *,
            CASE
                WHEN data_fim IS NOT NULL THEN data_fim
                WHEN DATE_ADD(data_inicio, INTERVAL 299 DAY) <= CURRENT_DATE() THEN DATE_ADD(data_inicio, INTERVAL 299 DAY)
                ELSE NULL
            END AS data_fim_efetiva,
            DATE_ADD(data_inicio, INTERVAL 40 WEEK) AS dpp
        FROM gestacoes_unicas
    ),

    -- ------------------------------------------------------------
    -- Fase e Trimestre da Gestação (inalterado)
    -- ------------------------------------------------------------
    filtrado AS (
        SELECT
            gcs.*,
            CASE
                WHEN gcs.data_fim IS NULL
                AND DATE_ADD(
                    gcs.data_inicio,
                    INTERVAL 299 DAY
                ) > CURRENT_DATE() THEN 'Gestação'
                WHEN gcs.data_fim IS NOT NULL
                AND DATE_DIFF (
                    CURRENT_DATE(),
                    gcs.data_fim,
                    DAY
                ) <= 45 THEN 'Puerpério'
                ELSE 'Encerrada'
            END AS fase_atual,
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
                ELSE 'Data inválida ou encerrada'
            END AS trimestre_atual_gestacao
        FROM gestacoes_com_status gcs
    ),

    -- ------------------------------------------------------------
    -- Equipe de Saúde da Família vigente na gestação (inalterado)
    -- ------------------------------------------------------------
    unnested_equipes AS (
        SELECT
            p.dados.id_paciente AS id_paciente,
            eq.datahora_ultima_atualizacao,
            eq.nome AS equipe_nome,
            eq.clinica_familia.nome AS clinica_nome
        FROM
            {{ ref('mart_historico_clinico__paciente') }} p
            left join UNNEST (p.equipe_saude_familia) AS eq
    ),
    equipe_durante_gestacao AS (
        SELECT f.id_gestacao,
            eq.equipe_nome, eq.clinica_nome, ROW_NUMBER() OVER (
                PARTITION BY
                    f.id_gestacao
                ORDER BY eq.datahora_ultima_atualizacao DESC
            ) AS rn
        FROM
            filtrado f
                LEFT JOIN unnested_equipes eq ON f.id_paciente = eq.id_paciente
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
-- Finalização do Modelo (15 colunas — contrato inalterado)
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
