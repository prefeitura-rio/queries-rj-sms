{{
    config(
        enabled=true,
        alias="linha_tempo",
    )
}}

WITH 


-- CTE 9: filtrado 
-- Determina a 'fase_atual' da gestação (Gestação, Puerpério, Encerrada).
-- Esta CTE é central e será usada para juntar a maioria das outras informações.
filtrado AS (
    SELECT *
    FROM {{ ref('mart_bi_gestacoes__gestacoes') }}
),

-- CTE 10: condicoes_gestantes_raw
-- Coleta todas as condições (CIDs) e suas datas de diagnóstico para todos os pacientes.
-- Esta CTE será usada para identificar diversas condições (diabetes, hipertensão, etc.) através de JOINs ou subconsultas.
-- O filtro `situacao IN ('ATIVO', 'RESOLVIDO')` é importante.
-- *Otimização*: Removido filtro de `id_paciente IN (SELECT DISTINCT id_paciente FROM filtrado)` pois
-- as junções subsequentes já farão essa filtragem implicitamente.
condicoes_gestantes_raw AS (
    SELECT ea.paciente.id_paciente, c.id AS cid, SAFE.PARSE_DATE (
            '%Y-%m-%d', SUBSTR(c.data_diagnostico, 1, 10)
        ) AS data_diagnostico, c.situacao -- Mantido para referência, embora já filtrado
    FROM
        {{ ref('mart_historico_clinico__episodio') }} ea
        --Ajuste UNNEST (foi retirado a vírgula ao fim da linha acima)
        LEFT JOIN UNNEST (condicoes) c
    WHERE
        c.situacao IN ('ATIVO', 'RESOLVIDO')
        AND c.id IS NOT NULL -- Garante que o CID existe para o JOIN
        AND c.data_diagnostico IS NOT NULL
        AND c.data_diagnostico != ''
),

-- CTE 11: categorias_risco_gestacional
-- Agrega as categorias de risco distintas para cada gestação.
-- Junta 'filtrado' com 'episodio_assistencial' e 'cids_risco_gestacional' para encontrar riscos durante a gestação.
categorias_risco_gestacional AS (
    SELECT f.id_gestacao, STRING_AGG (
            DISTINCT r.categoria, '; '
            ORDER BY r.categoria
        ) AS categorias_risco
    FROM
        filtrado f -- Usa 'filtrado' que já tem 'id_gestacao' e as datas corretas
        JOIN {{ ref('mart_historico_clinico__episodio') }} ea ON f.id_paciente = ea.paciente.id_paciente
        -- Considera episódios que ocorreram durante o período da gestação
        AND ea.entrada_data BETWEEN f.data_inicio AND COALESCE(
            f.data_fim_efetiva,
            CURRENT_DATE()
        )
        --Ajuste UNNEST | Acrescentei somente o 'left'
        LEFT JOIN UNNEST (ea.condicoes) AS c
        JOIN {{ ref('raw_sheets__cids_risco_gestacional') }} r ON c.id = r.cid
    WHERE
        c.id IS NOT NULL -- Redundante se r.cid não puder ser NULL, mas seguro
    GROUP BY
        f.id_gestacao
),

-- CTE 12: pacientes_info
-- Unifica a obtenção de dados do paciente e cálculo da faixa etária para evitar múltiplas leituras da tabela `paciente`.
-- Usa ROW_NUMBER para deduplicar pacientes caso haja múltiplos registros para o mesmo id_paciente, priorizando pelo `cpf_particao`.
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

-- Agrega todos os CNS distintos para cada paciente em uma string.
pacientes_todos_cns AS (
    SELECT p.dados.id_paciente, STRING_AGG (
            DISTINCT cns_individual, '; '
            ORDER BY cns_individual
        ) AS cns_string
    FROM
        {{ ref('mart_historico_clinico__paciente') }} p
        -- Ajuste UNNEST (foi retirado a vírgula ao fim da linha acima)
        LEFT JOIN UNNEST (p.cns) AS cns_individual
    WHERE
        cns_individual IS NOT NULL
        AND cns_individual != ''
    GROUP BY
        p.dados.id_paciente
),

-- CTE 13: unnested_equipes
-- Prepara os dados de equipe, desaninhando o array `equipe_saude_familia` uma única vez.
-- Isso pode ser reutilizado para encontrar equipe durante e anterior à gestação.
unnested_equipes AS (
    SELECT
        p.dados.id_paciente AS id_paciente,
        eq.datahora_ultima_atualizacao,
        eq.nome AS equipe_nome,
        eq.clinica_familia.nome AS clinica_nome
    FROM
        {{ ref('mart_historico_clinico__paciente') }} p
        -- Ajuste UNNEST (foi retirado a vírgula ao fim da linha acima)
        LEFT JOIN UNNEST (p.equipe_saude_familia) AS eq
),

-- CTE 14: equipe_durante_gestacao
-- Identifica a equipe de saúde mais recente do paciente DURANTE o período da gestação.
-- Usa 'filtrado' para as datas da gestação e 'unnested_equipes' para os dados da equipe.
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

-- CTE 15: equipe_durante_final
-- Filtra para pegar apenas a equipe mais recente durante a gestação.
equipe_durante_final AS (
    SELECT
        id_gestacao,
        equipe_nome,
        clinica_nome
    FROM equipe_durante_gestacao
    WHERE
        rn = 1
),

-- CTE 16: equipe_anterior_gestacao
-- Identifica a equipe de saúde mais recente do paciente ANTES do início da gestação.
equipe_anterior_gestacao AS (
    SELECT
        f.id_gestacao, -- Chave para JOIN posterior
        eq.equipe_nome AS equipe_nome_anterior, -- Renomeado para evitar conflito
        ROW_NUMBER() OVER (
            PARTITION BY
                f.id_gestacao
            ORDER BY eq.datahora_ultima_atualizacao DESC
        ) AS rn
    FROM
        filtrado f
        LEFT JOIN unnested_equipes eq ON f.id_paciente = eq.id_paciente
        -- A equipe deve ter sido atualizada ESTRITAMENTE ANTES da data de início da gestação
        AND DATE(
            eq.datahora_ultima_atualizacao
        ) < f.data_inicio
),

-- CTE 17: equipe_anterior_final
-- Filtra para pegar apenas a equipe mais recente antes da gestação.
equipe_anterior_final AS (
    SELECT
        id_gestacao,
        equipe_nome_anterior
    FROM equipe_anterior_gestacao
    WHERE
        rn = 1
),

-- CTE 18: mudanca_equipe
-- Verifica se houve mudança de equipe comparando a equipe anterior com a equipe durante a gestação.
mudanca_equipe AS (
    SELECT
        d.id_gestacao,
        CASE
            WHEN COALESCE(d.equipe_nome, '') <> COALESCE(a.equipe_nome_anterior, '') THEN 1
            ELSE 0
        END AS mudanca_equipe_durante_pn
    FROM
        equipe_durante_final d
        LEFT JOIN equipe_anterior_final a ON d.id_gestacao = a.id_gestacao
),

-- CTE 19: eventos_parto
-- Identifica eventos de parto ou aborto, filtrando por CIDs específicos e pelo fornecedor 'vitai'.
eventos_parto AS (
    SELECT
        ea.paciente.id_paciente,
        ea.entrada_data AS data_parto,
        ea.estabelecimento.nome AS estabelecimento_parto,
        ea.motivo_atendimento AS motivo_atencimento_parto, -- "atencimento" parece um typo, mas mantendo como na original
        ea.desfecho_atendimento AS desfecho_atendimento_parto,
        CASE
            WHEN c.id LIKE 'O8[0-4]%'
            OR c.id LIKE 'Z37%'
            OR c.id LIKE 'Z39%' THEN 'Parto' -- Ajustado para O80-O84
            WHEN c.id LIKE 'O0[0-4]%' THEN 'Aborto' -- Ajustado para O00-O04
            ELSE 'Outro' -- Pode ser Z38 (Nascido vivo) se não coberto por Z37 (Resultado do parto)
        END AS tipo_parto,
        c.id as cid_parto -- Para depuração ou análise mais fina
    FROM
        {{ ref('mart_historico_clinico__episodio') }} ea
        -- Ajuste UNNEST (foi retirado a vírgula ao fim da linha acima)
        LEFT JOIN UNNEST (ea.condicoes) AS c
    WHERE
        ea.entrada_data >= DATE '2021-01-01' -- Filtro de data para relevância
        AND LOWER(ea.prontuario.fornecedor) = 'vitai' -- Filtro específico de fornecedor
        AND (
            c.id LIKE 'O0[0-4]%'
            OR -- Aborto
            c.id LIKE 'O8[0-4]%'
            OR -- Parto
            c.id LIKE 'Z37%'
            OR -- Resultado do parto
            c.id LIKE 'Z38%'
            OR -- Nascido vivo (geralmente associado ao recém-nascido, mas pode estar no prontuário da mãe)
            c.id LIKE 'Z39%' -- Cuidado e exame pós-parto
        )
),

-- CTE 20: partos_associados
-- Associa o evento de parto/aborto mais próximo à data de fim efetiva da gestação.
-- Usa ARRAY_AGG com LIMIT 1 para selecionar o evento de parto cuja data é mais próxima da data_fim_efetiva.
partos_associados AS (
 SELECT
   f.id_gestacao, -- Chave para JOIN
   -- Pega o evento de parto mais próximo (dentro da janela) à data de fim efetiva
   ARRAY_AGG(
     STRUCT(e.data_parto, e.tipo_parto, e.estabelecimento_parto, e.motivo_atencimento_parto, e.desfecho_atendimento_parto)
     ORDER BY ABS(DATE_DIFF(e.data_parto, f.data_fim_efetiva, DAY))
     LIMIT 1
   )[OFFSET(0)] AS evento_parto_associado
 FROM filtrado f
 JOIN eventos_parto e
   ON f.id_paciente = e.id_paciente
   -- Janela para associar o parto: desde o início da gestação até 15 dias após o fim efetivo.
   AND e.data_parto BETWEEN f.data_inicio AND DATE_ADD(COALESCE(f.data_fim_efetiva, f.dpp, CURRENT_DATE()), INTERVAL 15 DAY)
   -- Adicionado COALESCE para data_fim_efetiva para casos de gestação em aberto.
 GROUP BY
   f.id_gestacao
),

-- CTEs de Agregação (presumivelmente de tabelas fato pré-calculadas, o que é bom)
-- CTE 21: consultas_prenatal
consultas_prenatal AS (
    SELECT
        id_gestacao,
        COUNT(*) AS total_consultas_prenatal
    FROM
        {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }}
    GROUP BY
        id_gestacao
),

-- CTE 22: status_prescricoes
status_prescricoes AS (
    SELECT
        id_gestacao,
        MAX(
            CASE
                WHEN REGEXP_CONTAINS (LOWER(prescricoes), r'f[oó]lico') THEN 'sim'
                ELSE 'não'
            END
        ) AS prescricao_acido_folico,
        MAX(
            CASE
                WHEN REGEXP_CONTAINS (LOWER(prescricoes),r'c[aá]lcio') THEN 'sim'
                ELSE 'não'
            END
        ) AS prescricao_carbonato_calcio
    FROM
        {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }}
    GROUP BY
        id_gestacao
),

-- CTE 23: ultima_consulta_prenatal
ultima_consulta_prenatal AS (
    SELECT
        id_gestacao,
        MAX(data_consulta) AS data_ultima_consulta
    FROM
        {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }}
    GROUP BY
        id_gestacao
),

-- CTE 24: visitas_acs_por_gestacao
visitas_acs_por_gestacao AS (
    SELECT
        id_gestacao,
        COUNT(*) AS total_visitas_acs
    FROM
        {{ ref('mart_bi_gestacoes__visitas_acs_gestacao') }}
    GROUP BY
        id_gestacao
),

-- CTE 25: ultima_visita_acs
ultima_visita_acs AS (
    SELECT
        id_gestacao,
        MAX(entrada_data) AS data_ultima_visita
    FROM
        {{ ref('mart_bi_gestacoes__visitas_acs_gestacao') }}
    GROUP BY
        id_gestacao
),

-- CTE 26: maior_pa_por_gestacao
maior_pa_por_gestacao AS (
    SELECT *
    FROM (
            SELECT
                id_gestacao, pressao_sistolica, pressao_diastolica, data_consulta, ROW_NUMBER() OVER (
                    PARTITION BY
                        id_gestacao
                    ORDER BY
                        pressao_sistolica DESC, pressao_diastolica DESC -- Pega a PA mais alta
                ) AS rn
            FROM
                {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }}
            WHERE
                pressao_sistolica IS NOT NULL
                AND pressao_diastolica IS NOT NULL -- Garante que ambas existem
        )
    WHERE
        rn = 1
),

-- CTE 27: condicoes_flags
-- *Otimização*: Esta CTE substitui as múltiplas subconsultas `EXISTS` do SELECT final.
-- Ela junta `filtrado` com `condicoes_gestantes_raw` uma vez e calcula todas as flags de condição.
condicoes_flags AS (
    SELECT
        f.id_gestacao, -- Chave para o GROUP BY e para o JOIN final
        -- Diabetes Prévio: CID E10-E14 ou O24.0-O24.3 ANTES do fim efetivo da gestação
        MAX(
            CASE
                WHEN (
                    cg.cid LIKE 'E1[0-4]%'
                    OR cg.cid LIKE 'O24[0-3]%'
                )
                AND cg.data_diagnostico < COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                ) THEN 1
                ELSE 0
            END
        ) AS diabetes_previo,
        -- Diabetes Gestacional: CID O24.4 DURANTE a gestação
        MAX(
            CASE
                WHEN cg.cid = 'O244'
                AND cg.data_diagnostico BETWEEN f.data_inicio AND COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                )  THEN 1
                ELSE 0
            END
        ) AS diabetes_gestacional,
        -- Diabetes Não Especificado: CID O24.9 DURANTE a gestação
        MAX(
            CASE
                WHEN cg.cid = 'O249'
                AND cg.data_diagnostico BETWEEN f.data_inicio AND COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                )  THEN 1
                ELSE 0
            END
        ) AS diabetes_nao_especificado,
        -- Hipertensão Prévia: CID I10-I15 ou O10 ANTES do fim efetivo da gestação
        MAX(
            CASE
                WHEN (
                    cg.cid LIKE 'I1[0-5]%'
                    OR cg.cid LIKE 'O10%'
                )
                AND cg.data_diagnostico < COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                ) THEN 1
                ELSE 0
            END
        ) AS hipertensao_previa,
        -- Pré-eclâmpsia: CID O11 ou O14 DURANTE a gestação
        MAX(
            CASE
                WHEN (
                    cg.cid LIKE 'O11%'
                    OR cg.cid LIKE 'O14%'
                )
                AND cg.data_diagnostico BETWEEN f.data_inicio AND COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                )  THEN 1
                ELSE 0
            END
        ) AS preeclampsia,
        -- Hipertensão Não Especificada: CID O16 DURANTE a gestação
        MAX(
            CASE
                WHEN cg.cid = 'O16'
                AND cg.data_diagnostico BETWEEN f.data_inicio AND COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                )  THEN 1
                ELSE 0
            END
        ) AS hipertensao_nao_especificada,
        -- HIV: CID B20-B24 ou Z21 ATÉ o fim efetivo da gestação
        MAX(
            CASE
                WHEN (
                    cg.cid LIKE 'B2[0-4]%'
                    OR cg.cid = 'Z21'
                )
                AND cg.data_diagnostico <= COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                ) THEN 1
                ELSE 0
            END
        ) AS hiv,
        -- Sífilis: CID A51-A53 (considerar A50 para congênita se relevante) um pouco antes ou DURANTE a gestação
        MAX(
            CASE
                WHEN cg.cid LIKE 'A5[1-3]%'
                AND cg.data_diagnostico BETWEEN DATE_SUB(
                    f.data_inicio,
                    INTERVAL 30 DAY
                ) AND COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                )  THEN 1
                ELSE 0
            END
        ) AS sifilis,
        -- Tuberculose: CID A15-A19 um pouco antes ou DURANTE a gestação
        MAX(
            CASE
                WHEN cg.cid LIKE 'A1[5-9]%'
                AND cg.data_diagnostico BETWEEN DATE_SUB(
                    f.data_inicio,
                    INTERVAL 6 MONTH
                ) AND COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                )  THEN 1
                ELSE 0
            END
        ) AS tuberculose
        ,
        -- Doenças autoimunes por CID (LES M32; SAF D68.6/D686) durante a gestação
        MAX(
            CASE
                WHEN (
                    cg.cid LIKE 'M32%'
                    OR cg.cid = 'D686'
                    OR cg.cid = 'D68.6'
                )
                AND cg.data_diagnostico BETWEEN f.data_inicio AND COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                ) THEN 1 ELSE 0
            END
        ) AS doenca_autoimune_cid,
        -- Reprodução assistida (Z312, Z313, Z318, Z319) durante a gestação
        MAX(
            CASE
                WHEN cg.cid IN ('Z312','Z313','Z318','Z319')
                AND cg.data_diagnostico BETWEEN f.data_inicio AND COALESCE(
                    f.data_fim_efetiva,
                    f.dpp,
                    CURRENT_DATE()
                ) THEN 1 ELSE 0
            END
        ) AS reproducao_assistida_cid
        -- Hipertensão Categorias de Risco Gestacional(INCLUIR)
    FROM
        filtrado f
        LEFT JOIN condicoes_gestantes_raw cg ON f.id_paciente = cg.id_paciente
    GROUP BY
        f.id_gestacao -- Agrupa para obter uma linha por gestação com todas as flags
),

-- ========================================
-- BLOCO: ANÁLISE DE HIPERTENSÃO (NOVO)
-- ========================================

-- CTE 28: analise_pressao_arterial
-- Análise detalhada das medições de PA
analise_pressao_arterial AS (
    SELECT
        f.id_gestacao,
        fapn.data_consulta,
        fapn.pressao_sistolica,
        fapn.pressao_diastolica,
        -- PA alterada (≥140/90)
        CASE
            WHEN fapn.pressao_sistolica >= 140
            OR fapn.pressao_diastolica >= 90 THEN 1
            ELSE 0
        END AS pa_alterada,
        -- PA grave (>160/110)
        CASE
            WHEN fapn.pressao_sistolica > 160
            OR fapn.pressao_diastolica > 110 THEN 1
            ELSE 0
        END AS pa_grave,
        -- PA controlada (<140/90)
        CASE
            WHEN fapn.pressao_sistolica < 140
            AND fapn.pressao_diastolica < 90 THEN 1
            ELSE 0
        END AS pa_controlada
    FROM
        filtrado f
        LEFT JOIN {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }} fapn ON f.id_gestacao = fapn.id_gestacao
    WHERE
        fapn.pressao_sistolica IS NOT NULL
        AND fapn.pressao_diastolica IS NOT NULL
),

-- CTE 29: resumo_controle_pressorico
-- Resume o controle pressórico por gestação
resumo_controle_pressorico AS (
    SELECT
        id_gestacao,
        -- Quantidade de PAs alteradas
        COUNT(
            CASE
                WHEN pa_alterada = 1 THEN 1
            END
        ) AS qtd_pas_alteradas,
        -- Se teve PA grave
        MAX(pa_grave) AS teve_pa_grave,
        -- Total de medições
        COUNT(*) AS total_medicoes_pa,
        -- Percentual de atendimentos com controle (<140x90)
        ROUND(
            COUNT(
                CASE
                    WHEN pa_controlada = 1 THEN 1
                END
            ) * 100.0 / COUNT(*),
            1
        ) AS percentual_pa_controlada
    FROM analise_pressao_arterial
    GROUP BY
        id_gestacao
),

-- CTE 30: ultima_pa_aferida
-- Pega a última PA aferida de cada gestação
ultima_pa_aferida AS (
    SELECT *
    FROM (
            SELECT
                id_gestacao, data_consulta AS data_ultima_pa, pressao_sistolica AS ultima_sistolica, pressao_diastolica AS ultima_diastolica, pa_controlada AS ultima_pa_controlada, ROW_NUMBER() OVER (
                    PARTITION BY
                        id_gestacao
                    ORDER BY data_consulta DESC
                ) AS rn
            FROM analise_pressao_arterial
        )
    WHERE
        rn = 1
),

-- CTE 31: prescricoes_anti_hipertensivos
-- Identifica prescrições de anti-hipertensivos
prescricoes_anti_hipertensivos AS (
    SELECT
        f.id_gestacao,
        -- Medicamentos individuais
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%METILDOPA%' THEN 1
                ELSE 0
            END
        ) AS tem_metildopa,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%HIDRALAZINA%' THEN 1
                ELSE 0
            END
        ) AS tem_hidralazina,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%NIFEDIPINA%' THEN 1
                ELSE 0
            END
        ) AS tem_nifedipina,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%HIDROCLOROTIAZIDA%' THEN 1
                ELSE 0
            END
        ) AS tem_hidroclorotiazida,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%ANLODIPINA%' THEN 1
                ELSE 0
            END
        ) AS tem_anlodipina,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%LOSARTANA%' THEN 1
                ELSE 0
            END
        ) AS tem_losartana,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%ENALAPRIL%' THEN 1
                ELSE 0
            END
        ) AS tem_enalapril,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%PROPRANOLOL%' THEN 1
                ELSE 0
            END
        ) AS tem_propranolol,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%FUROSEMIDA%' THEN 1
                ELSE 0
            END
        ) AS tem_furosemida,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%ATENOLOL%' THEN 1
                ELSE 0
            END
        ) AS tem_atenolol,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%CAPTOPRIL%' THEN 1
                ELSE 0
            END
        ) AS tem_captopril,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%CARVEDILOL%' THEN 1
                ELSE 0
            END
        ) AS tem_carvedilol,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%VERAPAMIL%' THEN 1
                ELSE 0
            END
        ) AS tem_verapamil,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%ESPIRONOLACTONA%' THEN 1
                ELSE 0
            END
        ) AS tem_espironolactona,
        -- Flag geral de anti-hipertensivo
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%METILDOPA%'
                OR UPPER(fapn.prescricoes) LIKE '%HIDRALAZINA%'
                OR UPPER(fapn.prescricoes) LIKE '%NIFEDIPINA%'
                OR UPPER(fapn.prescricoes) LIKE '%HIDROCLOROTIAZIDA%'
                OR UPPER(fapn.prescricoes) LIKE '%ANLODIPINA%'
                OR UPPER(fapn.prescricoes) LIKE '%LOSARTANA%'
                OR UPPER(fapn.prescricoes) LIKE '%ENALAPRIL%'
                OR UPPER(fapn.prescricoes) LIKE '%PROPRANOLOL%'
                OR UPPER(fapn.prescricoes) LIKE '%FUROSEMIDA%'
                OR UPPER(fapn.prescricoes) LIKE '%ATENOLOL%'
                OR UPPER(fapn.prescricoes) LIKE '%CAPTOPRIL%'
                OR UPPER(fapn.prescricoes) LIKE '%CARVEDILOL%'
                OR UPPER(fapn.prescricoes) LIKE '%VERAPAMIL%'
                OR UPPER(fapn.prescricoes) LIKE '%ESPIRONOLACTONA%' THEN 1
                ELSE 0
            END
        ) AS tem_anti_hipertensivo
    FROM
        filtrado f
        LEFT JOIN {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }} fapn ON f.id_gestacao = fapn.id_gestacao
    WHERE
        fapn.prescricoes IS NOT NULL
        AND fapn.prescricoes != ''
    GROUP BY
        f.id_gestacao
),

-- CTE 32: classificacao_anti_hipertensivos
-- Classifica anti-hipertensivos por adequação à gestação
classificacao_anti_hipertensivos AS (
 SELECT
   pah.id_gestacao,
   pah.tem_anti_hipertensivo,
   -- SEGUROS/ADEQUADOS na gestação
   CASE
     WHEN pah.tem_metildopa = 1
       OR pah.tem_hidralazina = 1
       OR pah.tem_nifedipina = 1
     THEN 1 ELSE 0
   END AS tem_anti_hipertensivo_seguro,

-- CONTRAINDICADOS/USO COM CAUTELA na gestação
CASE
    WHEN pah.tem_enalapril = 1
    OR pah.tem_captopril = 1
    OR pah.tem_losartana = 1
    OR pah.tem_atenolol = 1
    OR pah.tem_propranolol = 1
    OR pah.tem_carvedilol = 1
    OR pah.tem_anlodipina = 1
    OR pah.tem_verapamil = 1
    OR pah.tem_hidroclorotiazida = 1
    OR pah.tem_furosemida = 1
    OR pah.tem_espironolactona = 1 THEN 1
    ELSE 0
END AS tem_anti_hipertensivo_contraindicado,

-- Listas separadas
STRING_AGG(DISTINCT
     CASE
       WHEN pah.tem_metildopa = 1 THEN 'METILDOPA'
       WHEN pah.tem_hidralazina = 1 THEN 'HIDRALAZINA'
       WHEN pah.tem_nifedipina = 1 THEN 'NIFEDIPINA'
     END, '; '
   ) AS anti_hipertensivos_seguros,
  
   STRING_AGG(DISTINCT
     CASE
       WHEN pah.tem_enalapril = 1 THEN 'ENALAPRIL'
       WHEN pah.tem_captopril = 1 THEN 'CAPTOPRIL'
       WHEN pah.tem_losartana = 1 THEN 'LOSARTANA'
       WHEN pah.tem_atenolol = 1 THEN 'ATENOLOL'
       WHEN pah.tem_propranolol = 1 THEN 'PROPRANOLOL'
       WHEN pah.tem_carvedilol = 1 THEN 'CARVEDILOL'
       WHEN pah.tem_anlodipina = 1 THEN 'ANLODIPINA'
       WHEN pah.tem_verapamil = 1 THEN 'VERAPAMIL'
       WHEN pah.tem_hidroclorotiazida = 1 THEN 'HIDROCLOROTIAZIDA'
       WHEN pah.tem_furosemida = 1 THEN 'FUROSEMIDA'
       WHEN pah.tem_espironolactona = 1 THEN 'ESPIRONOLACTONA'
     END, '; '
   ) AS anti_hipertensivos_contraindicados
 FROM prescricoes_anti_hipertensivos pah
 GROUP BY
   pah.id_gestacao,
   pah.tem_anti_hipertensivo,
   pah.tem_metildopa,
   pah.tem_hidralazina,
   pah.tem_nifedipina,
   pah.tem_enalapril,
   pah.tem_captopril,
   pah.tem_losartana,
   pah.tem_atenolol,
   pah.tem_propranolol,
   pah.tem_carvedilol,
   pah.tem_anlodipina,
   pah.tem_verapamil,
   pah.tem_hidroclorotiazida,
   pah.tem_furosemida,
   pah.tem_espironolactona
),

-- CTE 33: prescricoes_antidiabeticos
-- Identifica prescrições de antidiabéticos
prescricoes_antidiabeticos AS (
    SELECT
        f.id_gestacao,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%METFORMINA%'
                OR UPPER(fapn.prescricoes) LIKE '%INSULINA%'
                OR UPPER(fapn.prescricoes) LIKE '%GLIBENCLAMIDA%'
                OR UPPER(fapn.prescricoes) LIKE '%GLICLAZIDA%' THEN 1
                ELSE 0
            END
        ) AS tem_antidiabetico,
        STRING_AGG (
            DISTINCT CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%METFORMINA%' THEN 'METFORMINA'
                WHEN UPPER(fapn.prescricoes) LIKE '%INSULINA%' THEN 'INSULINA'
                WHEN UPPER(fapn.prescricoes) LIKE '%GLIBENCLAMIDA%' THEN 'GLIBENCLAMIDA'
                WHEN UPPER(fapn.prescricoes) LIKE '%GLICLAZIDA%' THEN 'GLICLAZIDA'
            END,
            '; '
        ) AS antidiabeticos_lista
    FROM
        filtrado f
        LEFT JOIN {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }} fapn ON f.id_gestacao = fapn.id_gestacao
    WHERE
        fapn.prescricoes IS NOT NULL
    GROUP BY
        f.id_gestacao
),

-- CTE 34: encaminhamento_hipertensao_sisreg
-- Identifica encaminhamentos para pré-natal de alto risco por hipertensão
encaminhamento_hipertensao_sisreg AS (
    SELECT
        f.id_gestacao,
        pi.cpf,
        s.paciente_cpf,
        s.cid_id,
        s.data_solicitacao,
        DATE(s.data_solicitacao) AS data_solicitacao_date,
        s.solicitacao_status,
        s.solicitacao_situacao,
        s.procedimento,
        s.procedimento_id,
        ROW_NUMBER() OVER (
            PARTITION BY
                f.id_gestacao
            ORDER BY s.data_solicitacao ASC
        ) as rn_encaminhamento_has
    FROM
        filtrado f
        JOIN pacientes_info pi ON f.id_paciente = pi.id_paciente
        LEFT JOIN {{ ref('raw_sisreg_api__solicitacoes') }} s ON pi.cpf = s.paciente_cpf
        AND s.procedimento_id = '0703844' -- CONSULTA EM OBSTETRICIA - ALTO RISCO GERAL
        AND (
            s.cid_id LIKE 'O10%'
            OR -- Hipertensão prévia
            s.cid_id LIKE 'I1[0-5]'
            OR -- Hipertensão essencial
            s.cid_id = 'O11'
            OR -- Pré-eclâmpsia superposta
            s.cid_id = 'O13'
            OR -- Hipertensão gestacional
            s.cid_id = 'O14'
            OR -- Pré-eclâmpsia
            s.cid_id = 'O15'
            OR -- Eclâmpsia
            s.cid_id = 'O16' -- Hipertensão não especificada
        )
        AND DATE(s.data_solicitacao) BETWEEN f.data_inicio AND COALESCE(
            f.data_fim_efetiva,
            CURRENT_DATE()
        )
    WHERE
        pi.cpf IS NOT NULL
        AND pi.cpf != ''
),

-- CTE 35: resumo_encaminhamento_has (CORRIGIDA)
resumo_encaminhamento_has AS (
    SELECT
        eh_sis.id_gestacao,
        1 AS tem_encaminhamento_has, -- Será 1 porque filtramos abaixo para apenas os que têm match
        MIN(eh_sis.data_solicitacao_date) AS data_primeiro_encaminhamento_has,
        STRING_AGG (DISTINCT eh_sis.cid_id, '; ') AS cids_encaminhamento_has
    FROM
        encaminhamento_hipertensao_sisreg eh_sis
    WHERE
        eh_sis.rn_encaminhamento_has = 1
        AND eh_sis.data_solicitacao IS NOT NULL -- ESSA É A CONDIÇÃO CRÍTICA FALTANTE!
        -- Garante que apenas gestações com um encaminhamento SISREG *real*
        -- que satisfez os critérios da CTE 34 (procedimento, CID de HAS, data)
        -- sejam consideradas. Se s.data_solicitacao fosse NULL na CTE 34,
        -- significa que não houve um match válido nos critérios do JOIN.
    GROUP BY
        eh_sis.id_gestacao
),

-- CTE 36: prescricao_aas
-- Identifica prescrição de ácido acetilsalicílico
prescricao_aas AS (
    SELECT
        f.id_gestacao,
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%ACIDO ACETILSALICILICO%' THEN 1
                ELSE 0
            END
        ) AS tem_prescricao_aas,
        -- Data da primeira prescrição
        MIN(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%ACIDO ACETILSALICILICO%' THEN fapn.data_consulta
            END
        ) AS data_primeira_prescricao_aas,
        -- Data da última prescrição
        MAX(
            CASE
                WHEN UPPER(fapn.prescricoes) LIKE '%ACIDO ACETILSALICILICO%' THEN fapn.data_consulta
            END
        ) AS data_ultima_prescricao_aas
    FROM
        filtrado f
        LEFT JOIN {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }} fapn ON f.id_gestacao = fapn.id_gestacao
    WHERE
        fapn.prescricoes IS NOT NULL
    GROUP BY
        f.id_gestacao
),

-- CTE 37: dispensacao_aparelho_pa
-- Identifica dispensação de aparelho de pressão arterial
dispensacao_aparelho_pa AS (
    SELECT
        f.id_gestacao,
        pi.cpf,
        MAX(
            CASE
                WHEN m.id_material = '65159513221' THEN 1
                ELSE 0
            END
        ) AS tem_aparelho_pa_dispensado,
        MIN(
            CASE
                WHEN m.id_material = '65159513221' THEN DATE(m.data_hora_evento) -- Corrigido: data_hora_movimento → data_hora_evento
            END
        ) AS data_primeira_dispensacao_pa,
        COUNT(
            CASE
                WHEN m.id_material = '65159513221' THEN 1
            END
        ) AS qtd_aparelhos_pa_dispensados
    FROM
        filtrado f
        JOIN pacientes_info pi ON f.id_paciente = pi.id_paciente
        LEFT JOIN {{ ref('mart_estoque__movimento') }} m ON pi.cpf = m.consumo_paciente_cpf -- Campo correto confirmado
        AND m.id_material = '65159513221'
        AND DATE(m.data_hora_evento) BETWEEN f.data_inicio AND COALESCE(
            f.data_fim_efetiva,
            CURRENT_DATE()
        ) -- Corrigido
    WHERE
        pi.cpf IS NOT NULL
        AND pi.cpf != ''
    GROUP BY
        f.id_gestacao,
        pi.cpf
),

-- CTE 38: fatores_risco_categorias
-- Extrai fatores de risco do campo categorias_risco
fatores_risco_categorias AS (
    SELECT
        f.id_gestacao,
        crg.categorias_risco,
        -- Doença renal (NEFROPATIAS)
        CASE
            WHEN UPPER(crg.categorias_risco) LIKE '%NEFROPATIAS%' THEN 1
            ELSE 0
        END AS doenca_renal_cat,
        -- Gravidez gemelar (GEMELARIDADE)
        CASE
            WHEN UPPER(crg.categorias_risco) LIKE '%GEMELARIDADE%' THEN 1
            ELSE 0
        END AS gravidez_gemelar_cat,
        -- Doença autoimune (COLAGENOSES)
        CASE
            WHEN UPPER(crg.categorias_risco) LIKE '%COLAGENOSES%' THEN 1
            ELSE 0
        END AS doenca_autoimune_cat
    FROM
        filtrado f
        LEFT JOIN categorias_risco_gestacional crg ON f.id_gestacao = crg.id_gestacao
),

-- CTE 39: hipertensao_gestacional_completa (LÓGICA DE provavel_hipertensa_sem_diagnostico AJUSTADA)
hipertensao_gestacional_completa AS (
 SELECT
   f.id_gestacao,
   -- Controle pressórico
   COALESCE(rcp.qtd_pas_alteradas, 0) AS qtd_pas_alteradas,
   COALESCE(rcp.teve_pa_grave, 0) AS teve_pa_grave,
   COALESCE(rcp.total_medicoes_pa, 0) AS total_medicoes_pa,
   rcp.percentual_pa_controlada,
   upa.data_ultima_pa,
   upa.ultima_sistolica,
   upa.ultima_diastolica,
   upa.ultima_pa_controlada,
   -- Medicamentos
   COALESCE(cah.tem_anti_hipertensivo, 0) AS tem_anti_hipertensivo,
   COALESCE(cah.tem_anti_hipertensivo_seguro, 0) AS tem_anti_hipertensivo_seguro,
   COALESCE(cah.tem_anti_hipertensivo_contraindicado, 0) AS tem_anti_hipertensivo_contraindicado,
   cah.anti_hipertensivos_seguros,
   cah.anti_hipertensivos_contraindicados,
   -- Encaminhamento SISREG
    -- Encaminhamento HAS (inclui provável hipertensa sem diagnóstico)
    CASE
      WHEN COALESCE(reh.tem_encaminhamento_has, 0) = 1
        OR (
          (
            COALESCE(rcp.qtd_pas_alteradas, 0) >= 2 -- 2 ou mais PAs alteradas (≥140/90)
            OR COALESCE(rcp.teve_pa_grave, 0) = 1   -- PA grave (>160/110)
            OR COALESCE(cah.tem_anti_hipertensivo, 0) = 1 -- prescrição de anti-hipertensivo
            OR COALESCE(dap.tem_aparelho_pa_dispensado, 0) = 1 -- aparelho de PA dispensado
          )
          AND COALESCE(cf.hipertensao_previa, 0) = 0
          AND COALESCE(cf.preeclampsia, 0) = 0
          AND COALESCE(cf.hipertensao_nao_especificada, 0) = 0
        )
      THEN 1 ELSE 0
    END AS tem_encaminhamento_has,
   reh.data_primeiro_encaminhamento_has,
   reh.cids_encaminhamento_has,

    -- Provável hipertensa sem diagnóstico (LÓGICA AJUSTADA PARA MAIOR CLAREZA E ROBUSTEZ)
    CASE
        WHEN
        -- CONDIÇÃO 1: Tem evidência SUGESTIVA de hipertensão.
        -- Cada COALESCE(..., 0) trata casos onde o LEFT JOIN não encontrou correspondência para id_gestacao,
        -- resultando em NULL para a flag, que então é convertida para 0.
        (
            COALESCE(rcp.qtd_pas_alteradas, 0) >= 2 -- 2 ou mais PAs alteradas (≥140/90 mmHg).
            -- Verifique se o limiar ">=2" (ou seja, 2 ou mais) é o clinicamente desejado.
            -- Algumas diretrizes consideram 2 ou mais medições.
            OR COALESCE(rcp.teve_pa_grave, 0) = 1 -- Ou teve PA grave (>160/110 mmHg).
            OR COALESCE(cah.tem_anti_hipertensivo, 0) = 1 -- Ou tem prescrição de anti-hipertensivo.
            OR COALESCE(reh.tem_encaminhamento_has, 0) = 1 -- Ou tem encaminhamento SISREG por HAS
            OR COALESCE(dap.tem_aparelho_pa_dispensado, 0) = 1 -- Ou tem aparelho de PA dispensado
            -- (a CTE resumo_encaminhamento_has já filtra por CIDs de HAS).
        )
        -- CONDIÇÃO 2: E NÃO tem diagnóstico formal de hipertensão registrado.
        AND COALESCE(cf.hipertensao_previa, 0) = 0 -- Sem CID de hipertensão prévia (I10-I15, O10).
        AND COALESCE(cf.preeclampsia, 0) = 0 -- Sem CID de pré-eclâmpsia/eclâmpsia (O11, O14, O15).
        AND COALESCE(
            cf.hipertensao_nao_especificada,
            0
        ) = 0 -- Sem CID de hipertensão gestacional ou não especificada (O13, O16).
        THEN 1
        ELSE 0
    END AS provavel_hipertensa_sem_diagnostico,

    -- AAS
    COALESCE(paas.tem_prescricao_aas, 0) AS tem_prescricao_aas,
    paas.data_primeira_prescricao_aas,
    paas.data_ultima_prescricao_aas,
    -- Aparelho PA
    COALESCE(dap.tem_aparelho_pa_dispensado, 0) AS tem_aparelho_pa_dispensado,
    dap.data_primeira_dispensacao_pa,
    COALESCE(dap.qtd_aparelhos_pa_dispensados, 0) AS qtd_aparelhos_pa_dispensados
 FROM filtrado f
 LEFT JOIN resumo_controle_pressorico rcp ON f.id_gestacao = rcp.id_gestacao
 LEFT JOIN ultima_pa_aferida upa ON f.id_gestacao = upa.id_gestacao
 LEFT JOIN classificacao_anti_hipertensivos cah ON f.id_gestacao = cah.id_gestacao
 LEFT JOIN resumo_encaminhamento_has reh ON f.id_gestacao = reh.id_gestacao
 LEFT JOIN prescricao_aas paas ON f.id_gestacao = paas.id_gestacao
 LEFT JOIN dispensacao_aparelho_pa dap ON f.id_gestacao = dap.id_gestacao
 LEFT JOIN condicoes_flags cf ON f.id_gestacao = cf.id_gestacao -- cf contém as flags de diagnóstico
),

-- CTE: obesidade_gestante (IMC > 30 por consulta ou IMC início)
obesidade_gestante AS (
  SELECT
    f.id_gestacao,
    MAX(
      CASE
        WHEN SAFE_CAST(fapn.imc_consulta AS FLOAT64) > 30
          OR SAFE_CAST(fapn.imc_inicio AS FLOAT64) >= 30
        THEN 1 ELSE 0
      END
    ) AS tem_obesidade
  FROM filtrado f
  LEFT JOIN {{ ref('mart_bi_gestacoes__atendimentos_prenatal_aps') }} fapn
    ON f.id_gestacao = fapn.id_gestacao
  GROUP BY f.id_gestacao
),

-- CTE: prenatal_risco_marcadores (via ACTO ↔ CPF → PRE_NATAL)
prenatal_risco_marcadores AS (
  SELECT
    f.id_gestacao,
    MAX(CASE WHEN pn.agraval_risco_prenatal_histo_obstet_anterior = 'Pré-eclampsia/Eclampsia' THEN 1 ELSE 0 END) AS hist_pre_eclampsia,
    MAX(CASE WHEN pn.agraval_risco_prenatal_gravidez_actual = 'Gravidez múltipla' THEN 1 ELSE 0 END) AS gestacao_multipla_prenatal,
    MAX(CASE WHEN pn.agraval_risco_prenatal_gravidez_actual = 'Hipertensão' THEN 1 ELSE 0 END) AS has_cronica_prenatal,
    MAX(CASE WHEN pn.agraval_risco_prenatal_histo_reprod = 'Paridade 0' THEN 1 ELSE 0 END) AS nuliparidade_prenatal
  FROM filtrado f
  JOIN pacientes_info pi ON f.id_paciente = pi.id_paciente
  LEFT JOIN `rj-sms.brutos_prontuario_vitacare_historico.acto` a
    ON pi.cpf = a.patient_cpf
  LEFT JOIN `rj-sms.brutos_prontuario_vitacare_historico.pre_natal` pn
    ON pn.id_prontuario_global = a.id_prontuario_global
  WHERE pi.cpf IS NOT NULL AND pi.cpf != ''
  GROUP BY f.id_gestacao
),

-- CTE 40: fatores_risco_pe_adequacao
-- Consolida fatores de risco e avalia adequação da prescrição de AAS

fatores_risco_pe_adequacao AS (
 SELECT
   f.id_gestacao,
   -- Fatores presentes (versão para regra AAS)
   CASE WHEN (cf.hipertensao_previa = 1 OR hgc.provavel_hipertensa_sem_diagnostico = 1 OR COALESCE(prm.has_cronica_prenatal,0) = 1) THEN 1 ELSE 0 END AS hipertensao_cronica_confirmada,
   CASE WHEN cf.diabetes_previo = 1 OR pad.tem_antidiabetico = 1 THEN 1 ELSE 0 END AS diabetes_previo_confirmado,
   frc.doenca_renal_cat,
   -- Autoimune por categorias OU por CID
   CASE WHEN COALESCE(frc.doenca_autoimune_cat, 0) = 1 OR COALESCE(cf.doenca_autoimune_cid, 0) = 1 THEN 1 ELSE 0 END AS doenca_autoimune_total,
   -- Gemelaridade: categoria ou marcação de pre_natal
   CASE WHEN COALESCE(frc.gravidez_gemelar_cat,0) = 1 OR COALESCE(prm.gestacao_multipla_prenatal,0) = 1 THEN 1 ELSE 0 END AS gravidez_gemelar_total,
   COALESCE(cf.reproducao_assistida_cid, 0) AS reproducao_assistida_cid,
   COALESCE(og.tem_obesidade, 0) AS tem_obesidade,
   COALESCE(prm.hist_pre_eclampsia, 0) AS hist_pre_eclampsia,

   -- Total legado para compatibilidade
   (CASE WHEN cf.hipertensao_previa = 1 AND cah.tem_anti_hipertensivo = 1 THEN 1 ELSE 0 END +
    CASE WHEN cf.diabetes_previo = 1 AND pad.tem_antidiabetico = 1 THEN 1 ELSE 0 END +
    COALESCE(frc.doenca_renal_cat,0) + COALESCE(frc.doenca_autoimune_cat,0) + COALESCE(frc.gravidez_gemelar_cat,0)) AS total_fatores_risco_pe,

   -- Indicação de AAS (nova regra): ≥1 alto OU ≥2 moderados
   CASE
     WHEN (
       (
         CASE WHEN COALESCE(prm.hist_pre_eclampsia,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (COALESCE(frc.gravidez_gemelar_cat,0) = 1 OR COALESCE(prm.gestacao_multipla_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(og.tem_obesidade,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (cf.hipertensao_previa = 1 OR hgc.provavel_hipertensa_sem_diagnostico = 1 OR COALESCE(prm.has_cronica_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN (cf.diabetes_previo = 1 OR COALESCE(pad.tem_antidiabetico,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(frc.doenca_renal_cat,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (COALESCE(frc.doenca_autoimune_cat,0) = 1 OR COALESCE(cf.doenca_autoimune_cid,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(cf.reproducao_assistida_cid,0) = 1 THEN 1 ELSE 0 END
       ) >= 1
       OR (
         CASE WHEN (f.numero_gestacao = 1 OR COALESCE(prm.nuliparidade_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(pi.idade_atual, 0) >= 35 THEN 1 ELSE 0 END
       ) >= 2
     ) THEN 1 ELSE 0
   END AS tem_indicacao_aas,

   -- Status da prescrição
   paas.tem_prescricao_aas, sp.prescricao_carbonato_calcio,

   -- Adequação (nova regra)
   CASE
     WHEN (
       (
         CASE WHEN COALESCE(prm.hist_pre_eclampsia,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (COALESCE(frc.gravidez_gemelar_cat,0) = 1 OR COALESCE(prm.gestacao_multipla_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(og.tem_obesidade,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (cf.hipertensao_previa = 1 OR hgc.provavel_hipertensa_sem_diagnostico = 1 OR COALESCE(prm.has_cronica_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN (cf.diabetes_previo = 1 OR COALESCE(pad.tem_antidiabetico,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(frc.doenca_renal_cat,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (COALESCE(frc.doenca_autoimune_cat,0) = 1 OR COALESCE(cf.doenca_autoimune_cid,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(cf.reproducao_assistida_cid,0) = 1 THEN 1 ELSE 0 END
       ) >= 1
       OR (
         CASE WHEN (f.numero_gestacao = 1 OR COALESCE(prm.nuliparidade_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(pi.idade_atual, 0) >= 35 THEN 1 ELSE 0 END
       ) >= 2
     ) AND paas.tem_prescricao_aas = 1 THEN 'Adequado - Com AAS'
     WHEN (
       (
         CASE WHEN COALESCE(prm.hist_pre_eclampsia,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (COALESCE(frc.gravidez_gemelar_cat,0) = 1 OR COALESCE(prm.gestacao_multipla_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(og.tem_obesidade,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (cf.hipertensao_previa = 1 OR hgc.provavel_hipertensa_sem_diagnostico = 1 OR COALESCE(prm.has_cronica_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN (cf.diabetes_previo = 1 OR COALESCE(pad.tem_antidiabetico,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(frc.doenca_renal_cat,0) = 1 THEN 1 ELSE 0 END +
         CASE WHEN (COALESCE(frc.doenca_autoimune_cat,0) = 1 OR COALESCE(cf.doenca_autoimune_cid,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(cf.reproducao_assistida_cid,0) = 1 THEN 1 ELSE 0 END
       ) >= 1
       OR (
         CASE WHEN (f.numero_gestacao = 1 OR COALESCE(prm.nuliparidade_prenatal,0) = 1) THEN 1 ELSE 0 END +
         CASE WHEN COALESCE(pi.idade_atual, 0) >= 35 THEN 1 ELSE 0 END
       ) >= 2
     ) AND COALESCE(paas.tem_prescricao_aas,0) = 0 THEN 'Inadequado - Sem AAS'
     ELSE 'Sem indicação'
   END AS adequacao_aas_pe
  
  FROM filtrado f
  LEFT JOIN condicoes_flags cf ON f.id_gestacao = cf.id_gestacao
  LEFT JOIN classificacao_anti_hipertensivos cah ON f.id_gestacao = cah.id_gestacao
  LEFT JOIN prescricoes_antidiabeticos pad ON f.id_gestacao = pad.id_gestacao
  LEFT JOIN fatores_risco_categorias frc ON f.id_gestacao = frc.id_gestacao
  LEFT JOIN prescricao_aas paas ON f.id_gestacao = paas.id_gestacao
  LEFT JOIN status_prescricoes sp ON f.id_gestacao = sp.id_gestacao
  LEFT JOIN hipertensao_gestacional_completa hgc ON f.id_gestacao = hgc.id_gestacao
  LEFT JOIN obesidade_gestante og ON f.id_gestacao = og.id_gestacao
  LEFT JOIN pacientes_info pi ON f.id_paciente = pi.id_paciente
  LEFT JOIN prenatal_risco_marcadores prm ON f.id_gestacao = prm.id_gestacao
 ),

-- ========================================
-- FIM DO BLOCO DE HIPERTENSÃO
-- ========================================

incluir_AP AS (
    SELECT pinfo.id_paciente, estab.area_programatica
        -- paciente.`equipe_saude_familia`[SAFE_OFFSET(0)].clinica_familia.id_cnes,
        -- paciente.`equipe_saude_familia`[SAFE_OFFSET(0)].id_ine
    FROM
        pacientes_info pinfo
        LEFT JOIN {{ ref('mart_historico_clinico__paciente') }} paciente ON pinfo.id_paciente = paciente.dados.id_paciente
        LEFT JOIN {{ ref('dim_estabelecimento') }} estab ON pinfo.id_cnes = estab.id_cnes
),

-- CTE encaminhamento_SISREG (permanece como a última versão corrigida)
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
            PARTITION BY
                f.id_gestacao
            ORDER BY s.data_solicitacao ASC, s.procedimento_id ASC
        ) as rn_solicitacao
    FROM
        filtrado f
        JOIN pacientes_info pi ON f.id_paciente = pi.id_paciente
        LEFT JOIN {{ ref('raw_sisreg_api__solicitacoes') }} s ON (
            (
                (
                    pi.cpf IS NOT NULL
                    AND pi.cpf != ''
                )
                AND (
                    s.paciente_cpf IS NOT NULL
                    AND s.paciente_cpf != ''
                )
                AND pi.cpf = s.paciente_cpf
            )
            -- OR
            -- ((pi.cns IS NOT NULL AND ARRAY_LENGTH(pi.cns) > 0) AND (s.paciente_cns IS NOT NULL AND s.paciente_cns != '') AND s.paciente_cns IN UNNEST(pi.cns))
        )
        AND s.procedimento_id IN (
            '0703844',
            '0703886',
            '0737024',
            '0710301',
            '0710128'
        )
        AND DATE(s.data_solicitacao) BETWEEN f.data_inicio AND COALESCE(
            f.data_fim_efetiva,
            CURRENT_DATE()
        )
),
Urgencia_e_emergencia as (
    SELECT *
    FROM ( -- Adicionado subquery para aplicar ROW_NUMBER e filtrar
            SELECT f.id_gestacao, fapn.data_consulta, fapn.motivo_atendimento, fapn.nome_estabelecimento,
                -- Adiciona um número de linha para cada consulta de emergência dentro de uma gestação, ordenado pela data
                ROW_NUMBER() OVER (
                    PARTITION BY
                        f.id_gestacao
                    ORDER BY fapn.data_consulta DESC
                ) as rn_ue
            FROM
                filtrado f
                -- Mudado para INNER JOIN pois só queremos linhas se houver uma consulta de emergência.
                -- Se quiser manter todas as gestações e apenas popular os campos de UE quando existirem,
                -- mantenha o LEFT JOIN aqui e faça o LEFT JOIN da CTE Urgencia_e_emergencia no SELECT final.
                -- Para o propósito de criar uma CTE que só tem UMA linha por gestação (se houver UEs),
                -- um INNER JOIN aqui e depois um LEFT JOIN da CTE na query final é mais limpo.
                -- No entanto, para manter a estrutura original de LEFT JOIN no final, vamos construir a CTE
                -- com LEFT JOIN e filtrar pelo rn.
                LEFT JOIN {{ ref('mart_bi_gestacoes__consultas_emergenciais') }} fapn ON f.id_gestacao = fapn.id_gestacao
                -- O filtro de data deve estar no JOIN ou no WHERE da fonte de dados de emergência
                AND DATE(fapn.data_consulta) BETWEEN f.data_inicio AND COALESCE(
                    f.data_fim_efetiva, f.dpp, CURRENT_DATE()
                )
            WHERE
                fapn.id_gestacao IS NOT NULL -- Garante que houve um match no LEFT JOIN para aplicar o ROW_NUMBER corretamente
        )
    WHERE
        rn_ue = 1 -- Seleciona apenas a primeira consulta de emergência
),

final AS (

    -- Consulta Final: Junta todas as informações preparadas nas CTEs
    SELECT
        f.id_paciente,
        pi.cpf,
        -- pi.cns,
        ptc.cns_string,
        pi.nome,
        pi.data_nascimento,
        DATE_DIFF (
            CURRENT_DATE(),
            pi.data_nascimento,
            YEAR
        ) AS idade_gestante,
        pi.faixa_etaria, -- Faixa etária atual, baseada em CURRENT_DATE (da CTE pacientes_info)
        pi.raca,
        f.numero_gestacao,
        f.id_gestacao,
        f.data_inicio,
        f.data_fim,
        f.data_fim_efetiva,
        f.dpp,
        f.fase_atual,
        f.trimestre_atual_gestacao AS trimestre, -- Usando o trimestre calculado em 'filtrado'
        CASE
            WHEN f.fase_atual IN ('Gestação') THEN DATE_DIFF (
                CURRENT_DATE(),
                f.data_inicio,
                WEEK
            )
            ELSE NULL
        END AS IG_atual_semanas,
        CASE
            WHEN f.fase_atual IN ('Encerrada', 'Puerpério')
            AND f.data_fim_efetiva IS NOT NULL THEN DATE_DIFF (
                f.data_fim_efetiva,
                f.data_inicio,
                WEEK
            )
            ELSE NULL
        END AS IG_final_semanas,

    -- Condições (agora vindo da CTE condicoes_flags)
    cf.diabetes_previo,
    cf.diabetes_gestacional,
    cf.diabetes_nao_especificado,
    -- Diabetes Total: Se qualquer um dos indicadores de diabetes for 1
    GREATEST(
        cf.diabetes_previo,
        cf.diabetes_gestacional,
        cf.diabetes_nao_especificado
    ) AS diabetes_total,
    cf.hipertensao_previa,
    cf.preeclampsia,
    cf.hipertensao_nao_especificada,
    -- Hipertensão Total: Se qualquer um dos indicadores de hipertensão for 1
    GREATEST(
        cf.hipertensao_previa,
        cf.preeclampsia,
        cf.hipertensao_nao_especificada
    ) AS hipertensao_total,
    -- ========================================
    -- NOVOS CAMPOS DE HIPERTENSÃO
    -- ========================================
    -- Controle pressórico
    hgc.qtd_pas_alteradas,
    hgc.teve_pa_grave,
    hgc.total_medicoes_pa,
    hgc.percentual_pa_controlada,
    hgc.data_ultima_pa,
    hgc.ultima_sistolica,
    hgc.ultima_diastolica,
    hgc.ultima_pa_controlada,
    -- Medicamentos
    hgc.tem_anti_hipertensivo,
    hgc.tem_anti_hipertensivo_seguro,
    hgc.tem_anti_hipertensivo_contraindicado,
    hgc.anti_hipertensivos_seguros,
    hgc.anti_hipertensivos_contraindicados,
    -- Provável hipertensa
    hgc.provavel_hipertensa_sem_diagnostico,
    -- Encaminhamento HAS
    hgc.tem_encaminhamento_has,
    hgc.data_primeiro_encaminhamento_has,
    hgc.cids_encaminhamento_has,
    -- AAS e aparelho PA
    hgc.tem_prescricao_aas,
    hgc.data_primeira_prescricao_aas,
    hgc.tem_aparelho_pa_dispensado,
    hgc.data_primeira_dispensacao_pa,
    hgc.qtd_aparelhos_pa_dispensados,
    -- Antidiabéticos
    pad.tem_antidiabetico,
    pad.antidiabeticos_lista,
    -- Fatores de risco
    frc.doenca_renal_cat,
    frc.doenca_autoimune_cat,
    frc.gravidez_gemelar_cat,
    -- Adequação AAS
    frpa.hipertensao_cronica_confirmada,
    frpa.diabetes_previo_confirmado,
    frpa.total_fatores_risco_pe,
    frpa.tem_indicacao_aas,
    frpa.adequacao_aas_pe,
    -- ========================================
    -- FIM DOS NOVOS CAMPOS DE HIPERTENSÃO
    -- ========================================
    cf.hiv,
    cf.sifilis,
    cf.tuberculose,
    crg.categorias_risco,
    pa_max.pressao_sistolica AS max_pressao_sistolica,
    pa_max.pressao_diastolica AS max_pressao_diastolica,
    pa_max.data_consulta AS data_max_pa,
    COALESCE(
        cp.total_consultas_prenatal,
        0
    ) AS total_consultas_prenatal,
    COALESCE(
        sp.prescricao_acido_folico,
        'não'
    ) AS prescricao_acido_folico,
    COALESCE(
        sp.prescricao_carbonato_calcio,
        'não'
    ) AS prescricao_carbonato_calcio,
    CASE
        WHEN ucp.data_ultima_consulta IS NOT NULL THEN DATE_DIFF (
            CURRENT_DATE(),
            ucp.data_ultima_consulta,
            DAY
        )
        ELSE NULL
    END AS dias_desde_ultima_consulta,
    CASE
        WHEN ucp.data_ultima_consulta IS NOT NULL
        AND DATE_DIFF (
            CURRENT_DATE(),
            ucp.data_ultima_consulta,
            DAY
        ) >= 30 THEN 'sim'
        ELSE 'não'
    END AS mais_de_30_sem_atd,
    COALESCE(v_acs.total_visitas_acs, 0) AS total_visitas_acs,
    uv_acs.data_ultima_visita,
    CASE
        WHEN uv_acs.data_ultima_visita IS NOT NULL THEN DATE_DIFF (
            CURRENT_DATE(),
            uv_acs.data_ultima_visita,
            DAY
        )
        ELSE NULL
    END AS dias_desde_ultima_visita_acs,
    pi.obito_indicador,
    pi.obito_data,
    iap.area_programatica,
    edf.clinica_nome,
    edf.equipe_nome,
    COALESCE(
        me.mudanca_equipe_durante_pn,
        0
    ) AS mudanca_equipe_durante_pn,
    pa.evento_parto_associado.data_parto,
    pa.evento_parto_associado.tipo_parto,
    pa.evento_parto_associado.estabelecimento_parto,
    pa.evento_parto_associado.motivo_atencimento_parto, -- Mantendo tipo se for assim na origem
    pa.evento_parto_associado.desfecho_atendimento_parto,
    CASE
        WHEN sis_sol.data_solicitacao_date IS NOT NULL THEN 'sim'
        ELSE 'não'
    END AS encaminhado_sisreg,
    sis_sol.data_solicitacao_date AS sisreg_primeira_data_solicitacao,
    sis_sol.solicitacao_status AS sisreg_primeira_status,
    sis_sol.solicitacao_situacao AS sisreg_primeira_situacao,
    sis_sol.procedimento AS sisreg_primeira_procedimento_nome,
    sis_sol.procedimento_id AS sisreg_primeira_procedimento_id,
    sis_sol.unidade_solicitante AS sisreg_primeira_unidade_solicitante,
    sis_sol.medico_solicitante AS sisreg_primeira_medico_solicitante,
    sis_sol.operador_solicitante_nome AS sisreg_primeira_operador_solicitante,
    CASE
        WHEN ue.data_consulta IS NOT NULL THEN 'sim'
        ELSE 'não'
    END AS Urg_Emrg,
    ue.data_consulta as ue_data_consulta,
    ue.motivo_atendimento as ue_motivo_atendimento,
    ue.nome_estabelecimento as ue_nome_estabelecimento
    FROM
        filtrado f
        LEFT JOIN pacientes_info pi ON f.id_paciente = pi.id_paciente
        -- Não precisa mais de 'faixa_etaria' e 'incluir_trimestre' como joins separados
        LEFT JOIN incluir_AP iap ON f.id_paciente = iap.id_paciente
        LEFT JOIN pacientes_todos_cns ptc ON f.id_paciente = ptc.id_paciente
        LEFT JOIN equipe_durante_final edf ON f.id_gestacao = edf.id_gestacao -- Mudado para id_gestacao
        LEFT JOIN mudanca_equipe me ON f.id_gestacao = me.id_gestacao -- Mudado para id_gestacao
        LEFT JOIN partos_associados pa ON f.id_gestacao = pa.id_gestacao -- Mudado para id_gestacao
        LEFT JOIN consultas_prenatal cp ON f.id_gestacao = cp.id_gestacao
        LEFT JOIN status_prescricoes sp ON f.id_gestacao = sp.id_gestacao
        LEFT JOIN ultima_consulta_prenatal ucp ON f.id_gestacao = ucp.id_gestacao
        LEFT JOIN visitas_acs_por_gestacao v_acs ON f.id_gestacao = v_acs.id_gestacao
        LEFT JOIN ultima_visita_acs uv_acs ON f.id_gestacao = uv_acs.id_gestacao
        LEFT JOIN maior_pa_por_gestacao pa_max ON f.id_gestacao = pa_max.id_gestacao
        LEFT JOIN categorias_risco_gestacional crg ON f.id_gestacao = crg.id_gestacao
        LEFT JOIN condicoes_flags cf ON f.id_gestacao = cf.id_gestacao -- Nova CTE para flags de condição
        -- ========================================
        -- NOVOS JOINS DE HIPERTENSÃO
        -- ========================================
        LEFT JOIN hipertensao_gestacional_completa hgc ON f.id_gestacao = hgc.id_gestacao
        LEFT JOIN prescricoes_antidiabeticos pad ON f.id_gestacao = pad.id_gestacao
        LEFT JOIN fatores_risco_categorias frc ON f.id_gestacao = frc.id_gestacao
        LEFT JOIN fatores_risco_pe_adequacao frpa ON f.id_gestacao = frpa.id_gestacao
        -- ========================================
        -- FIM DOS NOVOS JOINS
        -- ========================================
        LEFT JOIN (
            SELECT *
            FROM encaminhamento_SISREG
            WHERE
                rn_solicitacao = 1
        ) sis_sol ON f.id_gestacao = sis_sol.id_gestacao
        -- WHERE f.fase_atual = 'Gestação'
        -- Filtro aplicado no final
        LEFT JOIN Urgencia_e_emergencia ue ON f.id_gestacao = ue.id_gestacao
)

SELECT
    *
FROM final
WHERE fase_atual IN ('Gestação', 'Puerpério')