{{ config(
    schema = 'projeto_pic',
    alias = "eventos",
    materialized = "table"
) }}

WITH
    -- Público-alvo atual
    publico_atual AS (          
        SELECT DISTINCT cpf
        FROM {{ ref("mart_iplanrio_pic__publico_alvo") }}
        WHERE cpf IS NOT NULL
    ),

    -- GESTAÇÃO
    gestacao_fase AS (
        SELECT
            cpf,
            DATE(data_diagnostico) AS inicio_fase,
            DATE(
                IFNULL(
                    data_diagnostico_seguinte,
                    DATE_ADD(data_diagnostico, INTERVAL 300 DAY)
                )
            ) AS fim_fase,
            'Gestacao' AS tipo_publico
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY cpf ORDER BY data_diagnostico DESC
                ) AS rn
            FROM {{ ref('mart_linhas_cuidado__gestacoes') }}
            WHERE cpf IS NOT NULL
        )
        WHERE rn = 1
    ),

    -- PUERPÉRIO (42 dias após o parto)
    puerperio_fase AS (
        SELECT
            cpf,
            DATE(data_diagnostico_seguinte) AS inicio_fase,
            DATE_ADD(DATE(data_diagnostico_seguinte), INTERVAL 42 DAY) AS fim_fase,
            'Puerperio' AS tipo_publico
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY cpf ORDER BY data_diagnostico_seguinte DESC
                ) AS rn
            FROM {{ ref('mart_linhas_cuidado__gestacoes') }}
            WHERE cpf IS NOT NULL
            AND data_diagnostico_seguinte IS NOT NULL
            AND CURRENT_DATE() BETWEEN data_diagnostico_seguinte
                                    AND DATE_ADD(data_diagnostico_seguinte, INTERVAL 42 DAY)
        )
        WHERE rn = 1
    ),

    -- INFÂNCIA 
    infancia_fase AS (
        SELECT
            cpf,
            DATE(inicio) AS inicio_fase,
            DATE(fim) AS fim_fase,
            'Infancia' AS tipo_publico
        FROM {{ ref("mart_iplanrio_pic__publico_alvo") }}
        WHERE tipo_publico = 'Infancia'
        AND cpf IS NOT NULL
    ),

    todas_as_fases AS (
        SELECT * FROM gestacao_fase
        UNION ALL
        SELECT * FROM puerperio_fase
        UNION ALL
        SELECT * FROM infancia_fase
    ),

    -- VISITAS DOMICILIARES
    visitas_domiciliares AS (
        SELECT
            cpf,
            'Visita Domiciliar' AS tipo_evento,
            COALESCE(datahora_fim, datahora_inicio) AS dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE REGEXP_CONTAINS(tipo, r'(?i)visita')
        AND cpf IS NOT NULL AND cpf <> 'NAO TEM'
        AND cbo_profissional = '515105' -- apenas ACS
    ),

    -- CONSULTAS
    consultas AS (
        SELECT
            cpf,
            'Consulta' AS tipo_evento,
            COALESCE(datahora_fim, datahora_inicio) AS dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE cpf <> 'NAO TEM' AND NOT REGEXP_CONTAINS(tipo, r'(?i)visita')
    ),

    -- CONSULTAS MÉDICO/ENFERMEIRO
    consultas_medico_enfermeiro AS (
        SELECT
            cpf,
            'Consulta - Médico/Enfermeiro' AS tipo_evento,
            COALESCE(datahora_fim, datahora_inicio) AS dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE cpf <> 'NAO TEM' AND NOT REGEXP_CONTAINS(tipo, r'(?i)visita')
        AND (
                REGEXP_CONTAINS(normalize_and_casefold(cbo_descricao_profissional, NFKD), r"medico")
            OR REGEXP_CONTAINS(normalize_and_casefold(cbo_descricao_profissional, NFKD), r"enfermeiro")
        )
    ),

    -- TESTES RÁPIDOS
    testes_rapidos AS (
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
        JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING(id_prontuario_global)
        WHERE pc.co_procedimento IN (
            '0214010058','0214010040',
            '0214010074','0214010082',
            '0214010090','0214010104'
        )
        AND a.patient_cpf IS NOT NULL AND TRIM(a.patient_cpf) <> ''

        UNION ALL

        SELECT
            a.patient_cpf AS cpf,
            'Teste rápido - HIV',
            CAST(t.loaded_at AS DATETIME)
        FROM {{ ref("raw_prontuario_vitacare_historico__testerapido") }} t
        JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING(id_prontuario_global)
        WHERE a.patient_cpf IS NOT NULL
        AND COALESCE(t.resultado_teste_hiv1, t.resultado_teste_hiv1_positivo,
                    t.resultado_teste_hiv2, t.resultado_teste_hiv2_positivo) IS NOT NULL

        UNION ALL

        SELECT
            a.patient_cpf AS cpf,
            'Teste rápido - Sífilis',
            CAST(t.loaded_at AS DATETIME)
        FROM {{ ref("raw_prontuario_vitacare_historico__testerapido") }} t
        JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING(id_prontuario_global)
        WHERE a.patient_cpf IS NOT NULL
        AND COALESCE(t.resultado_teste_sifilis, t.resultado_teste_sifilis_positivo) IS NOT NULL

        UNION ALL

        SELECT
            a.patient_cpf AS cpf,
            'Teste rápido - Hepatite B',
            CAST(t.loaded_at AS DATETIME)
        FROM {{ ref("raw_prontuario_vitacare_historico__testerapido") }} t
        JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING(id_prontuario_global)
        WHERE a.patient_cpf IS NOT NULL
        AND COALESCE(t.resultado_teste_hepatite_b, t.resultado_teste_hepatite_b_positivo) IS NOT NULL

        UNION ALL

        SELECT
            a.patient_cpf AS cpf,
            'Teste rápido - Hepatite C',
            CAST(t.loaded_at AS DATETIME)
        FROM {{ ref("raw_prontuario_vitacare_historico__testerapido") }} t
        JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING(id_prontuario_global)
        WHERE a.patient_cpf IS NOT NULL
        AND COALESCE(t.resultado_teste_hepatite_c, t.resultado_teste_hepatite_c_positivo) IS NOT NULL
    ),

    -- VACINAS
    vacinacoes AS (
        SELECT
            cpf,
            CONCAT('Vacina - ', imuno, ' - ', tipo, ordem) AS tipo_evento,
            dthr
        FROM (
            SELECT 
                a.patient_cpf AS cpf,
                'Pentavalente' AS imuno,
                CASE WHEN dose LIKE '%eforço%' THEN 'R'
                    WHEN dose LIKE '%nica%' THEN 'U'
                    ELSE 'D' END AS tipo,
                CASE 
                    WHEN dose LIKE '%1%' THEN '1'
                    WHEN dose LIKE '%2%' THEN '2'
                    WHEN dose LIKE '%3%' THEN '3'
                    WHEN dose LIKE '%4%' THEN '4'
                    ELSE '' END AS ordem,
                CAST(v.data_aplicacao AS DATETIME) AS dthr
            FROM {{ ref("raw_prontuario_vitacare_historico__vacina") }} v
            JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a USING(id_prontuario_global)
            WHERE LOWER(normalize_and_casefold(v.dose, NFKD)) NOT IN ('dose unica', 'outro')
            AND v.cod_vacina IN ('DTP/HB/Hib', 'Hexa')

            UNION ALL

            SELECT
                paciente_cpf AS cpf,
                'Pentavalente', 
                CASE WHEN vacina_dose LIKE '%eforço%' THEN 'R'
                    WHEN vacina_dose LIKE '%nica%' THEN 'U'
                    ELSE 'D' END,
                CASE 
                    WHEN vacina_dose LIKE '%1%' THEN '1'
                    WHEN vacina_dose LIKE '%2%' THEN '2'
                    WHEN vacina_dose LIKE '%3%' THEN '3'
                    WHEN vacina_dose LIKE '%4%' THEN '4'
                    ELSE '' END,
                CAST(vacina_aplicacao_data AS DATETIME)
            FROM {{ ref("raw_sipni__vacinacao") }}
            WHERE paciente_cpf IS NOT NULL
            AND vacina_nome IN (
                    'Vacina penta (DTP/HepB/Hib)',
                    'Vacina penta acelular (DTPa/VIP/Hib)',
                    'Vacina hexa (DTPa/HepB/VIP/Hib)'
            )
        )
    ),

todos_os_eventos AS (
    SELECT * FROM visitas_domiciliares
    UNION ALL SELECT * FROM consultas
    UNION ALL SELECT * FROM consultas_medico_enfermeiro
    UNION ALL SELECT * FROM vacinacoes
    UNION ALL SELECT * FROM testes_rapidos
),

eventos_unificados AS (
    SELECT
        e.cpf,
        e.tipo_evento,
        DATE(e.dthr) AS data_evento,
        f.tipo_publico,
        f.inicio_fase,
        f.fim_fase,
        DATE_DIFF(DATE(e.dthr), f.inicio_fase, DAY) AS distancia_dias
    FROM todos_os_eventos e
    JOIN todas_as_fases f
      ON e.cpf = f.cpf
     AND DATE(e.dthr) BETWEEN f.inicio_fase AND f.fim_fase
    JOIN publico_atual p
      ON e.cpf = p.cpf
)

SELECT
    *,
    STRUCT(CURRENT_TIMESTAMP() AS ultima_atualizacao) AS metadados
FROM eventos_unificados