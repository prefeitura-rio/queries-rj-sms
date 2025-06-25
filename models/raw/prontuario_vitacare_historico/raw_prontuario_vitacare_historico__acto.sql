{{
    config(
        alias="acto", 
        materialized="incremental",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH

    source_atendimentos AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'acto') }} 
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
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            unidade_ap AS unidade_ap,
            {{ process_null('patient_cpf') }} AS patient_cpf,
            {{ process_null('patient_code') }} AS patient_code,
            {{ process_null('profissional_cns') }} AS profissional_cns,
            {{ process_null('profissional_cpf') }} AS profissional_cpf,
            {{ process_null('profissional_nome') }} AS profissional_nome,
            {{ process_null('profissional_cbo') }} AS profissional_cbo,
            {{ process_null('profissional_cbo_descricao') }} AS profissional_cbo_descricao,
            {{ process_null('profissional_equipe_nome') }} AS profissional_equipe_nome,
            {{ process_null('profissional_equipe_cod_ine') }} AS profissional_equipe_cod_ine,
            safe_cast({{ process_null('datahora_inicio_atendimento') }} as datetime) AS datahora_inicio_atendimento,
            safe_cast({{ process_null('datahora_fim_atendimento') }} as datetime) AS datahora_fim_atendimento,
            safe_cast({{ process_null('datahora_marcacao_atendimento') }} as datetime) AS datahora_marcacao_atendimento,
            tipo_consulta AS tipo_consulta,
            eh_coleta AS eh_coleta,
            {{ process_null('subjetivo_motivo') }} AS subjetivo_motivo,
            {{ process_null('plano_observacoes') }} AS plano_observacoes,
            {{ process_null('avaliacao_observacoes') }} AS avaliacao_observacoes,
            {{ process_null('objetivo_descricao') }} AS objetivo_descricao,
            {{ process_null('notas_observacoes') }} AS notas_observacoes,
            ut_id AS ut_id,
            realizado AS realizado,
            {{ process_null('tipo_atendimento') }} AS tipo_atendimento,
            

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM atendimentos_deduplicados
    )

SELECT
    *
FROM fato_atendimentos