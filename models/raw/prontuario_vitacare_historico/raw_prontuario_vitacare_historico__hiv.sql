{{
    config(
        alias="hiv", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

WITH

    source_hiv AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'hiv') }} 
    ),


      -- Using window function to deduplicate hiv
    hiv_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_hiv
        )
        WHERE rn = 1
    ),

    fato_hiv AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes,

            {{ process_null('aidstransfusao') }} AS aids_transfusao,
            safe_cast({{ process_null('aidsdatatransfusao') }} AS DATE) AS aids_data_transfusao,
            safe_cast({{ process_null('aidsidadetransfusao') }} AS INT) AS aids_idade_transfusao,
            {{ process_null('aidstoxicodependente') }} AS aids_toxicodependente,
            safe_cast({{ process_null('aidsdatainiciotoxicodependente') }} AS DATE) AS aids_data_inicio_toxicodependente,
            safe_cast({{ process_null('aidsidadeiniciotoxicodependente') }} AS INT) AS aids_idade_inicio_toxicodependente,
            {{ process_null('aidshomosexual') }} AS aids_homosexual,
            safe_cast({{ process_null('aidsdatainiciohomosexual') }} AS DATE) AS aids_data_inicio_homosexual,
            safe_cast({{ process_null('aidsidadeiniciohomosexual') }} AS INT) AS aids_idade_inicio_homosexual,
            {{ process_null('aidsheterosexual') }} AS aids_heterosexual,
            safe_cast({{ process_null('aidsdatainicioheterosexual') }} AS DATE) AS aids_data_inicio_heterosexual,
            safe_cast({{ process_null('aidsidadeinicioheterosexual') }} AS INT) AS aids_idade_inicio_heterosexual,
            {{ process_null('aidsbisexual') }} AS aids_bisexual,
            safe_cast({{ process_null('aidsdatainiciobisexual') }} AS DATE) AS aids_data_inicio_bisexual,
            safe_cast({{ process_null('aidsidadeiniciobisexual') }} AS INT) AS aids_idade_inicio_bisexual,
            {{ process_null('aidsparceirovih') }} AS aids_parceiro_vih,
            safe_cast({{ process_null('aidsdatainicioparceirovih') }} AS DATE) AS aids_data_inicio_parceiro_vih,
            safe_cast({{ process_null('aidsidadeinicioparceirovih') }} AS INT) AS aids_idade_inicio_parceiro_vih,
            {{ process_null('aidsobsepidm') }} AS aids_obs_epidemiologia,
            safe_cast({{ process_null('aidsdoencaatualano1teste') }} AS NUMERIC) AS aids_doenca_atual_ano_1_teste,
            safe_cast({{ process_null('aidsdoencaatualdtaprovavelinfec') }} AS DATE) AS aids_doenca_atual_data_provavel_infeccao,
            {{ process_null('aidsdoencaatualfebre') }} AS aids_doenca_atual_febre,
            {{ process_null('aidsdoencaatualastenia') }} AS aids_doenca_atual_astenia,
            {{ process_null('aidsdoencaatualanorexia') }} AS aids_doenca_atual_anorexia,
            safe_cast({{ process_null('aidsdoencaatualemagrecimento') }} AS NUMERIC) AS aids_doenca_atual_emagrecimento,
            {{ process_null('aidsdoencaatualhipersudorese') }} AS aids_doenca_atual_hipersudorese,
            {{ process_null('aidsdoencaatualerupcaocutanea') }} AS aids_doenca_atual_erupcao_cutanea,
            {{ process_null('aidsdoencaatualaltpele') }} AS aids_doenca_atual_alteracao_pele,
            {{ process_null('aidsdoencaatualaltmucosa') }} AS aids_doenca_atual_alteracao_mucosa,
            {{ process_null('aidsdoencaatualadenopatias') }} AS aids_doenca_atual_adenopatias,
            safe_cast({{ process_null('aidsdoencaatualadenotamanho') }} AS NUMERIC) AS aids_doenca_atual_adeno_tamanho,
            {{ process_null('aidsdoencaatualadenodolorosas') }} AS aids_doenca_atual_adeno_dolorosas,
            {{ process_null('aidsdoencaatualvomitos') }} AS aids_doenca_atual_vomitos,
            safe_cast({{ process_null('aidsdoencaatualdiarreia') }} AS NUMERIC) AS aids_doenca_atual_diarreia,
            {{ process_null('aidsdoencaatualdisfagia') }} AS aids_doenca_atual_disfagia,
            {{ process_null('aidsdoencaatualdorabdominal') }} AS aids_doenca_atual_dor_abdominal,
            {{ process_null('aidsdoencaatualtosse') }} AS aids_doenca_atual_tosse,
            {{ process_null('aidsdoencaatualexpectoracao') }} AS aids_doenca_atual_expectoracao,
            {{ process_null('aidsdoencaatualdispneia') }} AS aids_doenca_atual_dispneia,
            {{ process_null('aidsdoencaatualdortoraxica') }} AS aids_doenca_atual_dor_toraxica,
            {{ process_null('aidsdoencaatualcefaleias') }} AS aids_doenca_atual_cefaleias,
            {{ process_null('aidsdoencaatualpertvisuais') }} AS aids_doenca_atual_perturbacoes_visuais,
            {{ process_null('aidsdoencaatualaltcomportamento') }} AS aids_doenca_atual_alteracao_comportamento,
            {{ process_null('aidsdoencaatualvertigens') }} AS aids_doenca_atual_vertigens,
            {{ process_null('aidsdoencaatualaltesfincterianas') }} AS aids_doenca_atual_alteracoes_esfincterianas,
            {{ process_null('aidsdoencaatualaltsensibilidade') }} AS aids_doenca_atual_alteracoes_sensibilidade,
            {{ process_null('aidsdoencaatualaltmotorassup') }} AS aids_doenca_atual_alteracoes_motoras_superiores,
            {{ process_null('aidsdoencaatualaltmotorasinf') }} AS aids_doenca_atual_alteracoes_motoras_inferiores,
            safe_cast({{ process_null('aidsdoencaatualdtinicioqueixas') }} AS DATE) AS aids_doenca_atual_data_inicio_queixas,
            {{ process_null('aidsdoencaatualobs') }} AS aids_doenca_atual_obs,
            {{ process_null('aidstransfusaovertical') }} AS aids_transfusao_vertical,
            safe_cast({{ process_null('aidstransfusaoverticaldata') }} AS DATE) AS aids_transfusao_vertical_data,
            {{ process_null('aidshemofilico') }} AS aids_hemofilico,
            safe_cast({{ process_null('aidshemofilicodata') }} AS DATE) AS aids_hemofilico_data,
            {{ process_null('aidsacidentetrabalho') }} AS aids_acidente_trabalho,
            safe_cast({{ process_null('aidsacidentetrabalhodata') }} AS DATE) AS aids_acidente_trabalho_data,
            safe_cast({{ process_null('aidstempoemagrecimento') }} AS NUMERIC) AS aids_tempo_emagrecimento,
            {{ process_null('epidemiologiaignorado') }} AS epidemiologia_ignorado,
            {{ process_null('aidsobsnotas') }} AS aids_obs_notas,
            {{ process_null('aidsemterapiaantiretroviral') }} AS aids_em_terapia_antiretroviral,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM hiv_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_hiv
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado