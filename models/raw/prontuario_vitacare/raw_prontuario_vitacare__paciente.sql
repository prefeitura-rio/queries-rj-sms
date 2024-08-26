{{
    config(
        alias="paciente",
        materialized="table",
    )
}}

-- dbt run --select raw_prontuario_vitacare__paciente

WITH 
    patients AS (
        SELECT
            *,
            'rotineiro' as tipo 
        from {{ ref("raw_prontuario_vitacare__paciente_rotineiro") }}
        union all
        SELECT
            *,
            'historico' as tipo
        from {{ ref("raw_prontuario_vitacare__paciente_historico") }}
    ), 
    patients_with_cpf AS (
        SELECT * 
        FROM patients 
        WHERE cpf IS NOT NULL
    )
SELECT *
FROM patients_with_cpf