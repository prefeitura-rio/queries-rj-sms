{{
    config(
        alias="eventos",
        materialized="table"
    )
}}
with

    -- ------------------------------------------------------------
    -- Publico Alvo
    -- ------------------------------------------------------------
    publico_alvo AS (              
    SELECT
        cpf,
        DATE(inicio) AS inicio,
        DATE(fim) AS fim,
        tipo_publico
    FROM {{ ref("mart_iplanrio_pic__publico_alvo") }}
    WHERE cpf IS NOT NULL AND TRIM(cpf) <> ''
        AND inicio IS NOT NULL AND fim IS NOT NULL
        AND inicio <= fim
    ),

    -- ------------------------------------------------------------
    -- Eventos
    -- ------------------------------------------------------------
    visitas_domiciliares as (
        SELECT
            cpf, 
            tipo as tipo_evento, 
            coalesce(datahora_fim, datahora_inicio) as dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE tipo = 'Visita Domiciliar' and cpf <> 'NAO TEM'
    ),
    consultas as (
        SELECT 
            cpf, 
            'Consulta' as tipo_evento, 
            coalesce(datahora_fim, datahora_inicio) as dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE cpf <> 'NAO TEM' and tipo <> 'Visita Domiciliar'
    ),

    consultas_medico_enfermeiro as (
        SELECT 
            cpf, 
            'Consulta - Médico/Enfermeiro' as tipo_evento, 
            coalesce(datahora_fim, datahora_inicio) as dthr
        FROM {{ ref("raw_prontuario_vitacare__atendimento") }}
        WHERE cpf <> 'NAO TEM' and tipo <> 'Visita Domiciliar'
        and (
                regexp_contains(normalize_and_casefold(cbo_descricao_profissional, NFKD), r"medico")
                or regexp_contains(normalize_and_casefold(cbo_descricao_profissional, NFKD), r"enfermeiro")
            )
    ),

    testes_rapidos AS (
        -- Busca por todos os testes em procedimentos clinicos
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
        INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a
            USING(id_prontuario_global)
        WHERE pc.co_procedimento IN ( '0214010058','0214010040', '0214010074','0214010082', '0214010090','0214010104' )
          AND a.patient_cpf IS NOT NULL
          AND TRIM(a.patient_cpf) <> ''

        UNION ALL

        -- Busca pelos testes de sífilis e hepatites na tabela de testes rapidos (nao tem HIV)
        SELECT
        a.patient_cpf AS cpf,
        CASE
            WHEN COALESCE(t.resultado_teste_sifilis, t.resultado_teste_sifilis_positivo) IS NOT NULL
            THEN 'Teste rápido - Sífilis'
            WHEN COALESCE(t.resultado_teste_hepatite_b, t.resultado_teste_hepatite_b_positivo) IS NOT NULL
            THEN 'Teste rápido - Hepatite B'
            WHEN COALESCE(t.resultado_teste_hepatite_c, t.resultado_teste_hepatite_c_positivo) IS NOT NULL
            THEN 'Teste rápido - Hepatite C'
        END AS tipo_evento,
        CAST(t.loaded_at AS DATETIME) AS dthr
        FROM {{ ref("raw_prontuario_vitacare_historico__testerapido") }} t
        INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a
        USING (id_prontuario_global)
        WHERE a.patient_cpf IS NOT NULL
        AND TRIM(a.patient_cpf) <> ''
        AND (
            COALESCE(t.resultado_teste_sifilis, t.resultado_teste_sifilis_positivo) IS NOT NULL OR
            COALESCE(t.resultado_teste_hepatite_b, t.resultado_teste_hepatite_b_positivo) IS NOT NULL OR
            COALESCE(t.resultado_teste_hepatite_c, t.resultado_teste_hepatite_c_positivo) IS NOT NULL
        )
    ),

    vacinacoes_dirty as (
        SELECT 
            a.patient_cpf as cpf,

            -- IMUNO
            cod_vacina,
            CASE 
                WHEN cod_vacina = 'COV19-PFZ' or cod_vacina = 'COVID-19 MODERNA - SPIKEVAX' THEN  'COVID-19'
                WHEN cod_vacina = 'DTP / Hib' then 'DTP' -- Revisar isso. Ideia é facilitar a contagem do reforço de DTP
                ELSE cod_vacina
            END as imuno,

            -- DOSAGEM
            CASE 
                WHEN 'eforço' in dose THEN 'R'
                WHEN 'nica' in dose THEN 'U'
                ELSE  'D'
            END as tipo,
            CASE 
                WHEN '1' in dose THEN '1'
                WHEN '2' in dose THEN '2'
                WHEN '3' in dose THEN '3'
                WHEN '4' in dose THEN '4'
                ELSE ''
            END as ordem,

            -- DATA
            cast(v.data_aplicacao as datetime) as dthr
        FROM {{ ref("raw_prontuario_vitacare_historico__vacina") }} v 
            INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a using(id_prontuario_global)
        WHERE 
            cod_vacina is not null and 
            dose is not null -- Revisar isso. Seria ela uma dose única?
    ),
    vacinacoes as (
        SELECT 
            a.patient_cpf as cpf, 
            concat('Vacina - ', imuno, ' - ', tipo, ordem) as tipo_evento,
            dthr
        FROM vacinacoes_dirty
    ),

    eventos as (
        SELECT * FROM visitas_domiciliares
        UNION ALL
        SELECT * FROM consultas
        UNION ALL
        select * from consultas_medico_enfermeiro
        UNION ALL
        SELECT * FROM vacinacoes
        UNION ALL
        select * from testes_rapidos
    ),

    eventos_publico_alvo AS (
      SELECT
        e.cpf,
        e.tipo_evento,
        e.dthr,
        p.inicio AS data_referencia,                            
        p.tipo_publico,
        DATE_DIFF(DATE(e.dthr), p.inicio, DAY) AS distancia_dias,
        STRUCT(CURRENT_TIMESTAMP() AS ultima_atualizacao) AS metadados
      FROM eventos e
      JOIN publico_alvo p
        ON e.cpf = p.cpf
       AND DATE(e.dthr) BETWEEN p.inicio AND p.fim
    )
select
    distinct *
from eventos_publico_alvo