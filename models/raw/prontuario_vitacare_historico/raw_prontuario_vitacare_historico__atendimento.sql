{{
    config(
        alias="atendimentos", 
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
        FROM {{ source('brutos_vitacare_historic_staging', 'ATENDIMENTOS') }} 
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
            {{ remove_double_quotes('unidade_ap') }} AS unidade_ap,
            {{ remove_double_quotes('patient_cpf') }} AS patient_cpf,
            {{ remove_double_quotes('patient_code') }} AS patient_code,
            {{ remove_double_quotes('profissional_cns') }} AS profissional_cns,
            {{ remove_double_quotes('profissional_cpf') }} AS profissional_cpf,
            {{ remove_double_quotes('profissional_nome') }} AS profissional_nome,
            {{ remove_double_quotes('profissional_cbo') }} AS profissional_cbo,
            {{ remove_double_quotes('profissional_cbo_descricao') }} AS profissional_cbo_descricao,
            {{ process_null(remove_double_quotes('profissional_equipe_nome')) }} AS profissional_equipe_nome,
            {{ process_null(remove_double_quotes('profissional_equipe_cod_ine')) }} AS profissional_equipe_cod_ine,
            SAFE_CAST({{ remove_double_quotes('datahora_inicio_atendimento') }} AS DATETIME ) AS datahora_inicio_atendimento,
            SAFE_CAST({{ remove_double_quotes('datahora_fim_atendimento') }} AS DATETIME)AS datahora_fim_atendimento,
            SAFE_CAST({{ remove_double_quotes('datahora_marcacao_atendimento') }} AS DATETIME) AS datahora_marcacao_atendimento,
            {{ remove_double_quotes('eh_coleta') }} AS eh_coleta,
            {{ process_null(remove_double_quotes('subjetivo_motivo')) }} AS subjetivo_motivo,
            {{ process_null(remove_double_quotes('plano_observacoes')) }} AS plano_observacoes,
            {{ process_null(remove_double_quotes('avaliacao_observacoes')) }} AS avaliacao_observacoes,
            {{ process_null(remove_double_quotes('objetivo_descricao')) }} AS objetivo_descricao,
            {{ process_null(remove_double_quotes('notas_observacoes')) }} AS notas_observacoes,
            {{ remove_double_quotes('ut_id') }} AS ut_id,
            {{ remove_double_quotes('realizado') }} AS realizado,
            {{ remove_double_quotes('tipo_atendimento') }} AS tipo_atendimento,
            
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM atendimentos_deduplicados
    )

SELECT
    *
FROM fato_atendimentos