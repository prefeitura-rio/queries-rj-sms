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
    dim_procedimentos_clinicos as (
        select
            id_prontuario_global,
            to_json_string(
                array_agg(
                    struct(
                        co_procedimento,
                        no_procedimento AS procedimento_clinico
                    )
                )
            ) as procedimentos_clinicos
        from {{ ref("raw_prontuario_vitacare_historico__procedimentos_clinicos") }}
        group by id_prontuario_global
    ),

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
            a.id_prontuario_global,
            REPLACE(a.acto_id, '.0', '') AS id_prontuario_local,
            id_cnes ,

            unidade_ap AS unidade_ap,
            {{ process_null('patient_cpf') }} AS patient_cpf,
            {{ process_null('patient_code') }} AS patient_code,
            {{ process_null('profissional_cns') }} AS profissional_cns,
            {{ process_null('profissional_cpf') }} AS profissional_cpf,
            {{ process_null(proper_br('profissional_nome')) }} AS profissional_nome,
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
            {{ process_null('procedimentos_clinicos') }} AS procedimentos_clinicos,
            {{ process_null('objetivo_descricao') }} AS objetivo_descricao,
            {{ process_null('notas_observacoes') }} AS notas_observacoes,
            ut_id AS ut_id,
            realizado AS realizado,
            {{ process_null('tipo_atendimento') }} AS tipo_atendimento,
            

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM atendimentos_deduplicados a 
        LEFT JOIN dim_procedimentos_clinicos using (id_prontuario_global)
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_atendimentos
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado