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

    -- ------------------------------------------------------------
    -- Vacinacoes
    -- ------------------------------------------------------------

    -- VITACARE
    vacinacoes_vitacare_std as (
        SELECT 
            a.patient_cpf as cpf,

            -- IMUNO
            'Pentavalente' as imuno,

            -- DOSAGEM
            CASE 
                WHEN dose like '%eforço%' THEN 'R'
                WHEN dose like '%nica%' THEN 'U'
                ELSE  'D'
            END as tipo,
            CASE 
                WHEN dose like '%1%' THEN '1'
                WHEN dose like '%2%' THEN '2'
                WHEN dose like '%3%' THEN '3'
                WHEN dose like '%4%' THEN '4'
                ELSE ''
            END as ordem,

            -- DATA
            cast(v.data_aplicacao as datetime) as dthr
        FROM {{ ref("raw_prontuario_vitacare_historico__vacina") }} v 
            INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a using(id_prontuario_global)
        WHERE
            LOWER(normalize_and_casefold(v.dose, NFKD)) NOT IN ('dose unica', 'outro')
            AND v.cod_vacina IN (
                'DTP/HB/Hib',
                'Hexa'
            )
    ),

    -- SIPNI
    vacinacoes_sipni_std as (
        SELECT
            nu_cpf_paciente as cpf,

            -- IMUNO: Por enquanto, SI-PNI só tem Pentavalente e relacionados. Padronizando:
            'Pentavalente' as imuno,

            -- DOSAGEM
            CASE 
                WHEN ds_dose_vacina like '%eforço%' THEN 'R'
                WHEN ds_dose_vacina like '%nica%' THEN 'U'
                ELSE  'D'
            END as tipo,
            CASE 
                WHEN ds_dose_vacina like '%1%' THEN '1'
                WHEN ds_dose_vacina like '%2%' THEN '2'
                WHEN ds_dose_vacina like '%3%' THEN '3'
                WHEN ds_dose_vacina like '%4%' THEN '4'
                WHEN ds_dose_vacina like '%5%' THEN '5'
                WHEN ds_dose_vacina like '%6%' THEN '6'
                ELSE ''
            END as ordem,

            -- DATA
            cast(dt_vacina as datetime) as dthr
        FROM {{ ref("raw_sipni__vacinacao") }}
        WHERE 
            nu_cpf_paciente IS NOT NULL AND
            ds_vacina in (
                'Vacina penta (DTP/HepB/Hib)',
                'Vacina penta acelular (DTPa/VIP/Hib)',
                'Vacina hexa (DTPa/HepB/VIP/Hib)'
            )
    ),

    -- MERGE
    vacinacoes_merge as (
        SELECT * FROM vacinacoes_vitacare_std
        UNION ALL
        SELECT * FROM vacinacoes_sipni_std
    ),

    vacinacoes as (
        SELECT
            cpf, 
            concat('Vacina - ', imuno, ' - ', tipo, ordem) as tipo_evento,
            dthr
        FROM vacinacoes_merge
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