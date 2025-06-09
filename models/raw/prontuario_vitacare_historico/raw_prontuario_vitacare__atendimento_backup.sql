{{
    config(
        alias="atendimento_backup", 
        materialized="table",
        schema="brutos_vitacare_historic", 
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

WITH

    source_atendimentos AS (
        SELECT *
        FROM {{ source('brutos_prontuario_vitacare_historico', 'ATENDIMENTOS') }} 
    ),

    dim_equipe AS (
        SELECT
            {{ remove_double_quotes('n_ine') }} AS n_ine,
            {{ remove_double_quotes('codigo') }} AS codigo_equipe
        FROM {{ source('brutos_prontuario_vitacare_historico', 'EQUIPES') }}
    ),

    fato_atendimento AS (
        SELECT
            -- PKs e Chaves
            {{ remove_double_quotes('sa.acto_id') }} AS id_prontuario_local, 
            CONCAT(
                NULLIF({{ remove_double_quotes('sa.id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('sa.acto_id') }}, '')
            ) AS id_prontuario_global,
            {{ remove_double_quotes('sa.patient_cpf') }} AS cpf,
            {{ remove_double_quotes('sa.id_cnes') }} AS cnes_unidade, 

            -- Profissional
            {{ remove_double_quotes('sa.profissional_cns') }} AS cns_profissional,
            {{ remove_double_quotes('sa.profissional_cpf') }} AS cpf_profissional,
            {{ remove_double_quotes('sa.profissional_nome') }} AS nome_profissional,
            {{ remove_double_quotes('sa.profissional_cbo') }} AS cbo_profissional,
            {{ remove_double_quotes('sa.profissional_cbo_descricao') }} AS cbo_descricao_profissional,

            -- Equipe 
            dep.codigo_equipe AS cod_equipe_profissional,
            {{ remove_double_quotes('sa.profissional_equipe_cod_ine') }} AS cod_ine_equipe_profissional,
            {{ remove_double_quotes('sa.profissional_equipe_nome') }} AS nome_equipe_profissional,

            -- Dados da Consulta
            {{ remove_double_quotes('sa.tipo_atendimento') }} AS tipo,
            {{ remove_double_quotes('sa.eh_coleta') }} AS eh_coleta,
            safe_cast(sa.datahora_marcacao_atendimento as datetime) as datahora_marcacao,
            safe_cast(sa.datahora_inicio_atendimento as datetime) as datahora_inicio,
            safe_cast(sa.datahora_fim_atendimento as datetime) as datahora_fim,

            -- Campos Livres 
            {{ remove_double_quotes('sa.subjetivo_motivo') }} AS soap_subjetivo_motivo,
            {{ remove_double_quotes('sa.objetivo_descricao') }} AS soap_objetivo_descricao,
            {{ remove_double_quotes('sa.avaliacao_observacoes') }} AS soap_avaliacao_observacoes,
            NULL AS soap_plano_procedimentos_clinicos,
            {{ remove_double_quotes('sa.plano_observacoes') }} AS soap_plano_observacoes,
            {{ remove_double_quotes('sa.notas_observacoes') }} AS soap_notas_observacoes,

            -- Metadados
            safe_cast(sa.datahora_fim_atendimento as datetime) as updated_at,
            safe_cast(sa.extracted_at as datetime) as loaded_at


        FROM source_atendimentos sa
        LEFT JOIN dim_equipe dep
            ON {{ remove_double_quotes('sa.profissional_equipe_cod_ine') }} = dep.n_ine
    ),


    dim_alergias AS (
        SELECT
            CONCAT(NULLIF({{ remove_double_quotes('id_cnes') }}, ''), '.', NULLIF({{ remove_double_quotes('acto_id') }}, '')) AS id_prontuario_global,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT({{ remove_double_quotes('alergias_anamnese_descricao') }} AS descricao))
            ) AS alergias
        FROM {{ source('brutos_prontuario_vitacare_historico', 'ALERGIAS') }}
        GROUP BY 1
    ),

    dim_condicoes AS (
        SELECT
            CONCAT(NULLIF({{ remove_double_quotes('id_cnes') }}, ''), '.', NULLIF({{ remove_double_quotes('acto_id') }}, '')) AS id_prontuario_global,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT(
                    {{ remove_double_quotes('cod_cid10') }} AS cod_cid10, 
                    {{ remove_double_quotes('estado') }} AS estado,       
                    {{ remove_double_quotes('data_diagnostico') }} AS data_diagnostico 
                
                ))
            ) AS condicoes
        FROM {{ source('brutos_prontuario_vitacare_historico', 'CONDICOES') }}
        GROUP BY 1
    ),

    dim_encaminhamentos AS (
        SELECT
            CONCAT(NULLIF({{ remove_double_quotes('id_cnes') }}, ''), '.', NULLIF({{ remove_double_quotes('acto_id') }}, '')) AS id_prontuario_global,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT({{ remove_double_quotes('encaminhamento_especialidade') }} AS descricao))
            ) AS encaminhamentos
        FROM {{ source('brutos_prontuario_vitacare_historico', 'ENCAMINHAMENTOS') }}
        GROUP BY 1
    ),

    dim_indicadores AS (
        SELECT
            CONCAT(NULLIF({{ remove_double_quotes('id_cnes') }}, ''), '.', NULLIF({{ remove_double_quotes('acto_id') }}, '')) AS id_prontuario_global,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT(
                    {{ remove_double_quotes('indicadores_nome') }} AS nome,
                    {{ remove_double_quotes('valor') }} AS valor
                ))
            ) AS indicadores
        FROM {{ source('brutos_prontuario_vitacare_historico', 'INDICADORES') }}
        GROUP BY 1
    ),

    dim_vacinas AS (
        SELECT
            CONCAT(NULLIF({{ remove_double_quotes('id_cnes') }}, ''), '.', NULLIF({{ remove_double_quotes('acto_id') }}, '')) AS id_prontuario_global,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT(
                    {{ remove_double_quotes('nome_vacina') }} AS nome_vacina,
                    {{ remove_double_quotes('cod_vacina') }} AS cod_vacina,
                    {{ remove_double_quotes('dose') }} AS dose,
                    {{ remove_double_quotes('data_aplicacao') }} AS data_aplicacao
                
                ))
            ) AS vacinas
        FROM {{ source('brutos_prontuario_vitacare_historico', 'VACINAS') }}
        GROUP BY 1
    ),

    dim_prescricoes AS (
        SELECT
            CONCAT(NULLIF({{ remove_double_quotes('id_cnes') }}, ''), '.', NULLIF({{ remove_double_quotes('acto_id') }}, '')) AS id_prontuario_global,
            TO_JSON_STRING(
                ARRAY_AGG(STRUCT(
                    {{ remove_double_quotes('nome_medicamento') }} AS nome_medicamento,
                    {{ remove_double_quotes('cod_medicamento') }} AS cod_medicamento,
                    {{ remove_double_quotes('posologia') }} AS posologia,
                    {{ remove_double_quotes('quantidade') }} AS quantidade,
                    {{ remove_double_quotes('uso_continuado') }} AS uso_continuado
                ))
            ) AS prescricoes
        FROM {{ source('brutos_prontuario_vitacare_historico', 'PRESCRICOES') }}
        GROUP BY 1
    ),

    atendimentos_enriquecidos AS (
        SELECT
            fa.*,
            da.alergias AS alergias_anamnese,
            dc.condicoes AS condicoes,
            dv.vacinas AS vacinas,
            di.indicadores AS indicadores,
            de.encaminhamentos AS encaminhamentos,
            dp.prescricoes AS prescricoes,
            safe_cast(fa.datahora_fim as date) as data_particao,

        FROM fato_atendimento fa
        LEFT JOIN dim_alergias da ON fa.id_prontuario_global = da.id_prontuario_global
        LEFT JOIN dim_condicoes dc ON fa.id_prontuario_global = dc.id_prontuario_global
        LEFT JOIN dim_encaminhamentos de ON fa.id_prontuario_global = de.id_prontuario_global
        LEFT JOIN dim_indicadores di ON fa.id_prontuario_global = di.id_prontuario_global
        LEFT JOIN dim_vacinas dv ON fa.id_prontuario_global = dv.id_prontuario_global
        LEFT JOIN dim_prescricoes dp ON fa.id_prontuario_global = dp.id_prontuario_global
    )

SELECT
    -- Gera o id_hci f
    {{ dbt_utils.generate_surrogate_key(['ae.id_prontuario_global']) }} AS id_hci,
    ae.* 
FROM atendimentos_enriquecidos ae