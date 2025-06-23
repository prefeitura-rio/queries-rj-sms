{{
    config(
        alias="acto_id", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_atendimentos AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'acto_id') }} 
    ),


      -- Using window function to deduplicate atendimentos
    atendimentos_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_atendimentos
        )
        WHERE rn = 1
    ),

    fato_atendimentos AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            unidade_ap AS unidade_ap,
            {{ process_null('patient_cpf') }} AS patient_cpf,
            {{ process_null('patient_code') }} AS patient_code,
            profissional_cns AS profissional_cns,
            profissional_cpf AS profissional_cpf,
            profissional_nome AS profissional_nome,
            profissional_cbo AS profissional_cbo,
            profissional_cbo_descricao AS profissional_cbo_descricao,
            profissional_equipe_nome AS profissional_equipe_nome,
            profissional_equipe_cod_ine AS profissional_equipe_cod_ine,
            {{ process_null('datahora_inicio_atendimento') }} AS datahora_inicio_atendimento,
            {{ process_null('datahora_fim_atendimento') }} AS datahora_fim_atendimento,
            {{ process_null('datahora_marcacao_atendimento') }} AS datahora_marcacao_atendimento,
            tipo_consulta AS tipo_consulta,
            eh_coleta AS eh_coleta,
            subjetivo_motivo AS subjetivo_motivo,
            plano_observacoes AS plano_observacoes,
            avaliacao_observacoes AS avaliacao_observacoes,
            objetivo_descricao AS objetivo_descricao,
            notas_observacoes AS notas_observacoes,
            ut_id AS ut_id,
            realizado AS realizado,
            {{ process_null('tipo_atendimento') }} AS tipo_atendimento,
            
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM atendimentos_deduplicados
    )

SELECT
    *
FROM fato_atendimentos