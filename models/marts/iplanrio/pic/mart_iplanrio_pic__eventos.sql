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
        SELECT * FROM vacinacoes
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