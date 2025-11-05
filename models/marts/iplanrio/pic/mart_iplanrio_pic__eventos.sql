{{ config(
    schema = 'projeto_pic',
    alias = "eventos",
    materialized = "table"
) }}

WITH
    -- Público-alvo atual
    publico_atual AS (              
    SELECT
        cpf,
        DATE(inicio) AS inicio,
        DATE(fim) AS fim,
        tipo_publico
    FROM {{ ref("mart_iplanrio_pic__publico_alvo") }}
    WHERE cpf IS NOT NULL
    ),

    -- Gestações relacionadas ao puerpério atual
    gestacao_relacionada AS (
        SELECT
            p.cpf,
            g.data_diagnostico AS inicio,
            g.data_diagnostico_seguinte AS fim,
            'Gestacao' AS tipo_publico
        FROM {{ ref('mart_linhas_cuidado__gestacoes') }} g
        INNER JOIN publico_atual p
            ON g.cpf = p.cpf
        AND p.tipo_publico = 'Puerperio'
        AND g.data_diagnostico_seguinte = p.inicio
    ),

    -- Eventos
    -- VISITAS DOMICILIARES
    visitas_domiciliares AS (
        SELECT
            cpf, 
            tipo AS tipo_evento, 
            COALESCE(datahora_fim, datahora_inicio) AS dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE tipo = 'Visita Domiciliar'
        AND cpf <> 'NAO TEM'
    ),

    -- CONSULTAS
    consultas AS (
        SELECT 
            cpf, 
            'Consulta' AS tipo_evento, 
            COALESCE(datahora_fim, datahora_inicio) AS dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE cpf <> 'NAO TEM'
        AND tipo <> 'Visita Domiciliar'
    ),

    consultas_medico_enfermeiro AS (
        SELECT 
            cpf, 
            'Consulta - Médico/Enfermeiro' AS tipo_evento, 
            COALESCE(datahora_fim, datahora_inicio) AS dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE cpf <> 'NAO TEM'
        AND tipo <> 'Visita Domiciliar'
        AND (
                REGEXP_CONTAINS(normalize_and_casefold(cbo_descricao_profissional, NFKD), r"medico")
            OR REGEXP_CONTAINS(normalize_and_casefold(cbo_descricao_profissional, NFKD), r"enfermeiro")
        )
    ),

    -- TESTES RÁPIDOS
    testes_rapidos AS (
        -- Procedimentos clínicos (Vitacare)
        SELECT
            a.patient_cpf AS cpf,
            CASE pc.co_procedimento
                WHEN '0214010058' THEN 'Teste rápido - HIV'
                WHEN '0214010040' THEN 'Teste rápido - HIV'
                WHEN '0214010074' THEN 'Teste rápido - Sífilis'
                WHEN '0214010082' THEN 'Teste rápido - Sífilis'
                WHEN '0214010090' THEN 'Teste rápido - Hepatite C'
                WHEN '0214010104' THEN 'Teste rápido - Hepatite B'
            END AS tipo_evento,            
            CAST(pc.loaded_at AS DATETIME) AS dthr
        FROM {{ ref("raw_prontuario_vitacare_historico__procedimentos_clinicos") }} pc
        INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING(id_prontuario_global)
        WHERE pc.co_procedimento IN (
            '0214010058','0214010040','0214010074','0214010082','0214010090','0214010104'
        )
        AND a.patient_cpf IS NOT NULL
        AND TRIM(a.patient_cpf) <> ''

        UNION ALL

        -- Tabela de testes rápidos (Vitacare)
        SELECT
            a.patient_cpf AS cpf,
            CASE
                WHEN COALESCE(t.resultado_teste_hiv1, t.resultado_teste_hiv1_positivo) IS NOT NULL THEN 'Teste rápido - HIV 1'
                WHEN COALESCE(t.resultado_teste_hiv2, t.resultado_teste_hiv2_positivo) IS NOT NULL THEN 'Teste rápido - HIV 2'
                WHEN COALESCE(t.resultado_teste_sifilis, t.resultado_teste_sifilis_positivo) IS NOT NULL THEN 'Teste rápido - Sífilis'
                WHEN COALESCE(t.resultado_teste_hepatite_b, t.resultado_teste_hepatite_b_positivo) IS NOT NULL THEN 'Teste rápido - Hepatite B'
                WHEN COALESCE(t.resultado_teste_hepatite_c, t.resultado_teste_hepatite_c_positivo) IS NOT NULL THEN 'Teste rápido - Hepatite C'
            END AS tipo_evento,
            CAST(t.loaded_at AS DATETIME) AS dthr
        FROM {{ ref("raw_prontuario_vitacare_historico__testerapido") }} t
        INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING (id_prontuario_global)
        WHERE a.patient_cpf IS NOT NULL
        AND TRIM(a.patient_cpf) <> ''
        AND (
                COALESCE(t.resultado_teste_hiv1, t.resultado_teste_hiv1_positivo) IS NOT NULL OR
                COALESCE(t.resultado_teste_hiv2, t.resultado_teste_hiv2_positivo) IS NOT NULL OR
                COALESCE(t.resultado_teste_sifilis, t.resultado_teste_sifilis_positivo) IS NOT NULL OR
                COALESCE(t.resultado_teste_hepatite_b, t.resultado_teste_hepatite_b_positivo) IS NOT NULL OR
                COALESCE(t.resultado_teste_hepatite_c, t.resultado_teste_hepatite_c_positivo) IS NOT NULL
        )
    ),

    -- VACINAÇÕES
    vacinacoes_vitacare_std AS (
        SELECT 
            a.patient_cpf AS cpf,
            'Pentavalente' AS imuno,
            CASE 
                WHEN dose LIKE '%eforço%' THEN 'R'
                WHEN dose LIKE '%nica%' THEN 'U'
                ELSE 'D'
            END AS tipo,
            CASE 
                WHEN dose LIKE '%1%' THEN '1'
                WHEN dose LIKE '%2%' THEN '2'
                WHEN dose LIKE '%3%' THEN '3'
                WHEN dose LIKE '%4%' THEN '4'
                ELSE ''
            END AS ordem,
            CAST(v.data_aplicacao AS DATETIME) AS dthr
        FROM {{ ref("raw_prontuario_vitacare_historico__vacina") }} v
        INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING(id_prontuario_global)
        WHERE
            LOWER(normalize_and_casefold(v.dose, NFKD)) NOT IN ('dose unica', 'outro')
            AND v.cod_vacina IN ('DTP/HB/Hib', 'Hexa')
    ),

    -- SIPNI
    vacinacoes_sipni_std AS (
        SELECT
            nu_cpf_paciente AS cpf,
            'Pentavalente' AS imuno,
            CASE 
                WHEN ds_dose_vacina LIKE '%eforço%' THEN 'R'
                WHEN ds_dose_vacina LIKE '%nica%' THEN 'U'
                ELSE 'D'
            END AS tipo,
            CASE 
                WHEN ds_dose_vacina LIKE '%1%' THEN '1'
                WHEN ds_dose_vacina LIKE '%2%' THEN '2'
                WHEN ds_dose_vacina LIKE '%3%' THEN '3'
                WHEN ds_dose_vacina LIKE '%4%' THEN '4'
                WHEN ds_dose_vacina LIKE '%5%' THEN '5'
                WHEN ds_dose_vacina LIKE '%6%' THEN '6'
                ELSE ''
            END AS ordem,
            CAST(dt_vacina AS DATETIME) AS dthr
        FROM {{ ref("raw_sipni__vacinacao") }}
        WHERE 
            nu_cpf_paciente IS NOT NULL
            AND ds_vacina IN (
                'Vacina penta (DTP/HepB/Hib)',
                'Vacina penta acelular (DTPa/VIP/Hib)',
                'Vacina hexa (DTPa/HepB/VIP/Hib)'
            )
    ),

    vacinacoes AS (
        SELECT
            cpf,
            CONCAT('Vacina - ', imuno, ' - ', tipo, ordem) AS tipo_evento,
            dthr
        FROM vacinacoes_vitacare_std
        UNION ALL
        SELECT
            cpf,
            CONCAT('Vacina - ', imuno, ' - ', tipo, ordem) AS tipo_evento,
            dthr
        FROM vacinacoes_sipni_std
    ),

    -- CONSOLIDAÇÃO DE TODOS OS EVENTOS
    eventos AS (
        SELECT * FROM visitas_domiciliares
        UNION ALL SELECT * FROM consultas
        UNION ALL SELECT * FROM consultas_medico_enfermeiro
        UNION ALL SELECT * FROM vacinacoes
        UNION ALL SELECT * FROM testes_rapidos
    ),

    -- EVENTOS NA FASE ATUAL
    eventos_publico_atual AS (
        SELECT
            e.cpf,
            e.tipo_evento,
            e.dthr,
            p.tipo_publico,
            p.inicio AS inicio_fase,
            p.fim AS fim_fase
        FROM eventos e
        JOIN publico_atual p
        ON e.cpf = p.cpf
        AND DATE(e.dthr) BETWEEN p.inicio AND p.fim
    ),

    -- EVENTOS NA GESTAÇÃO RELACIONADA (PUERPÉRIO)
    eventos_gestacao_relacionada AS (
        SELECT
            e.cpf,
            e.tipo_evento,
            e.dthr,
            g.tipo_publico,
            g.inicio AS inicio_fase,
            g.fim AS fim_fase
        FROM eventos e
        JOIN gestacao_relacionada g
        ON e.cpf = g.cpf
        AND DATE(e.dthr) BETWEEN g.inicio AND g.fim
    ),

    eventos_unificados AS (
        SELECT * FROM eventos_publico_atual
        UNION ALL
        SELECT * FROM eventos_gestacao_relacionada
    )

SELECT
    cpf,
    tipo_evento,
    DATE(dthr) AS data_evento,
    tipo_publico,
    inicio_fase,
    fim_fase,
    DATE_DIFF(DATE(dthr), inicio_fase, DAY) AS distancia_dias,
    STRUCT(CURRENT_TIMESTAMP() AS ultima_atualizacao) AS metadados
FROM eventos_unificados
WHERE inicio_fase <= fim_fase