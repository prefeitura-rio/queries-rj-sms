{{
    config(
        alias="paciente",
        materialized="table",
    )
}}

-- dbt run --select raw_prontuario_vitacare__paciente

WITH patients AS (
    SELECT
        *,
        'rotineiro' as tipo 
    from {{ ref("raw_prontuario_vitacare__paciente_rotineiro") }}
    union all
    SELECT
        *,
        'historico' as tipo
    from {{ ref("raw_prontuario_vitacare__paciente_historico") }}
), patients_with_cpf AS (
    SELECT * FROM patients WHERE cpf IS NOT NULL
), registers_ranked AS (
    SELECT
        *,
        row_number() over (partition by cpf order by tipo desc) as rank
    FROM patients_with_cpf
), latest_registers AS (
    SELECT * FROM registers_ranked WHERE rank = 1
)
SELECT * EXCEPT (rank)
FROM latest_registers