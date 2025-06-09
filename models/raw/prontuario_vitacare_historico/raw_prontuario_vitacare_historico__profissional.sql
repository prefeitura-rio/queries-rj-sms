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
        FROM {{ source('brutos_prontuario_vitacare_historico', 'PROFISSIONAIS') }} 
    ),



    fato_profissionais AS (
        SELECT
            -- PKs e Chaves
            REPLACE({{ remove_double_quotes('prof_id') }}, '.0', '') AS prof_id,
            {{ process_null(remove_double_quotes('profissional_cns')) }} AS profissional_cns,
            {{ process_null(remove_double_quotes('profissional_cpf')) }} AS profissional_cpf,
            {{ process_null(remove_double_quotes('profissional_nome')) }} AS profissional_nome,
            {{ process_null(remove_double_quotes('n_registro')) }} AS n_registro,
            {{ process_null(remove_double_quotes('profissional_cbo')) }} AS profissional_cbo,
            {{ process_null(remove_double_quotes('profissional_cbo_descricao')) }} AS profissional_cbo_descricao,
            {{ process_null(remove_double_quotes('profissional_equipe_nome')) }} AS profissional_equipe_nome,
            {{ process_null(remove_double_quotes('profissional_equipe_cod_equipe')) }} AS profissional_equipe_cod_equipe,
            {{ process_null(remove_double_quotes('profissional_equipe_cod_ine')) }} AS profissional_equipe_cod_ine,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at

        FROM source_profissionais
    )

SELECT
    *
FROM fato_profissionais