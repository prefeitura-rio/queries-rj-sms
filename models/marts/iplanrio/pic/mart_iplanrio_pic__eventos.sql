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
    publico_alvo as (
        SELECT
            cpf,
            data_referencia,
            tipo_publico
        FROM {{ ref("mart_iplanrio_pic__publico_alvo") }}
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

    -- Evento para consulta com medico ou enfermeiro
        consultas_medico_enfermeiro as (
        select 
            cpf, 
            'Consulta - Médico/Enfermeiro' as tipo_evento,
            coalesce(datahora_fim, datahora_inicio) as dthr
        from {{ ref("raw_prontuario_vitacare__atendimento") }}
        where cpf is not null and trim(cpf) <> ''
            and tipo <> 'Visita Domiciliar'
            and (
                regexp_contains(normalize_and_casefold(cbo_descricao_profissional, NFKD), r"medico")
                or regexp_contains(normalize_and_casefold(cbo_descricao_profissional, NFKD), r"enfermeiro")
            )
    ),

    -- Evento para testes rapidos de IST
    testes_rapidos as (
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
        WHERE pc.co_procedimento IN (
            '0214010058','0214010040',
            '0214010074','0214010082',
            '0214010090','0214010104'
        )
        AND a.patient_cpf IS NOT NULL
        AND TRIM(a.patient_cpf) <> ''
    ),

    vacinacoes as (
        SELECT 
            a.patient_cpf as cpf, 
            concat('Vacina - ', ifnull(cod_vacina, '<vacina sem cod>'), ' - ', ifnull(dose, '<vacina sem dose>')) as tipo_evento,
            cast(v.data_aplicacao as datetime) as dthr
        FROM {{ ref("raw_prontuario_vitacare_historico__vacina") }} v 
            INNER JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} a using(id_prontuario_global)
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
    eventos_publico_alvo as (
        SELECT 
            *,
            date_diff(eventos.dthr, publico_alvo.data_referencia, day) as distancia_dias,
            struct(
                current_timestamp() as ultima_atualizacao
            ) as metadados
        FROM eventos
            INNER JOIN publico_alvo using (cpf)
    )
select
    distinct *
from eventos_publico_alvo