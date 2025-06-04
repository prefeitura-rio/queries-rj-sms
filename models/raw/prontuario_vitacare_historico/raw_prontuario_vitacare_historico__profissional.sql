{{
    config(
        alias="profissionais", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_profissionais AS (
        SELECT 
            *
        FROM {{ source('brutos_vitacare_historic_staging', 'PROFISSIONAIS') }} 
    ),



    fato_profissionais AS (
        SELECT
            -- PKs e Chaves
            {{ remove_double_quotes('prof_id') }} AS prof_id,
            {{ remove_double_quotes('profissional_cns') }} AS profissional_cns,
            {{ remove_double_quotes('profissional_cpf') }} AS profissional_cpf,
            {{ remove_double_quotes('profissional_nome') }} AS profissional_nome,
            {{ remove_double_quotes('n_registro') }} AS n_registro,
            {{ remove_double_quotes('profissional_cbo') }} AS profissional_cbo,
            {{ remove_double_quotes('profissional_cbo_descricao') }} AS profissional_cbo_descricao,
            {{ remove_double_quotes('profissional_equipe_nome') }} AS profissional_equipe_nome,
            {{ remove_double_quotes('profissional_equipe_cod_equipe') }} AS profissional_equipe_cod_equipe,
            {{ remove_double_quotes('profissional_equipe_cod_ine') }} AS profissional_equipe_cod_ine,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at

        FROM source_profissionais
    )

SELECT
    *
FROM fato_profissionais