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
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
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
        id_cnes AS cnes_unidade,

         {{ process_null('aidstransfusao') }} AS aidstransfusao,
        safe_cast({{ process_null('aidsdatatransfusao') }} AS DATE) AS aidsdatatransfusao,
        safe_cast({{ process_null('aidsidadetransfusao') }} AS INT) AS aidsidadetransfusao,
        {{ process_null('aidstoxicodependente') }} AS aidstoxicodependente,
        safe_cast({{ process_null('aidsdatainiciotoxicodependente') }} AS DATE) AS aidsdatainiciotoxicodependente,
        safe_cast({{ process_null('aidsidadeiniciotoxicodependente') }} AS INT) AS aidsidadeiniciotoxicodependente,
        {{ process_null('aidshomosexual') }} AS aidshomosexual,
        safe_cast({{ process_null('aidsdatainiciohomosexual') }} AS DATE) AS aidsdatainiciohomosexual,
        safe_cast({{ process_null('aidsidadeiniciohomosexual') }} AS INT) AS aidsidadeiniciohomosexual,
        {{ process_null('aidsheterosexual') }} AS aidsheterosexual,
        safe_cast({{ process_null('aidsdatainicioheterosexual') }} AS DATE) AS aidsdatainicioheterosexual,
        safe_cast({{ process_null('aidsidadeinicioheterosexual') }} AS INT) AS aidsidadeinicioheterosexual,
        {{ process_null('aidsbisexual') }} AS aidsbisexual,
        safe_cast({{ process_null('aidsdatainiciobisexual') }} AS DATE) AS aidsdatainiciobisexual,
        safe_cast({{ process_null('aidsidadeiniciobisexual') }} AS INT) AS aidsidadeiniciobisexual,
        {{ process_null('aidsparceirovih') }} AS aidsparceirovih,
        safe_cast({{ process_null('aidsdatainicioparceirovih') }} AS DATE) AS aidsdatainicioparceirovih,
        safe_cast({{ process_null('aidsidadeinicioparceirovih') }} AS INT) AS aidsidadeinicioparceirovih,
        {{ process_null('aidsobsepidm') }} AS aidsobsepidm,
        safe_cast({{ process_null('aidsdoencaatualano1teste') }} AS NUMERIC) AS aidsdoencaatualano1teste,
        safe_cast({{ process_null('aidsdoencaatualdtaprovavelinfec') }} AS DATE) AS aidsdoencaatualdtaprovavelinfec,
        {{ process_null('aidsdoencaatualfebre') }} AS aidsdoencaatualfebre,
        {{ process_null('aidsdoencaatualastenia') }} AS aidsdoencaatualastenia,
        {{ process_null('aidsdoencaatualanorexia') }} AS aidsdoencaatualanorexia,
        safe_cast({{ process_null('aidsdoencaatualemagrecimento') }} AS NUMERIC) AS aidsdoencaatualemagrecimento,
        {{ process_null('aidsdoencaatualhipersudorese') }} AS aidsdoencaatualhipersudorese,
        {{ process_null('aidsdoencaatualerupcaocutanea') }} AS aidsdoencaatualerupcaocutanea,
        {{ process_null('aidsdoencaatualaltpele') }} AS aidsdoencaatualaltpele,
        {{ process_null('aidsdoencaatualaltmucosa') }} AS aidsdoencaatualaltmucosa,
        {{ process_null('aidsdoencaatualadenopatias') }} AS aidsdoencaatualadenopatias,
        safe_cast({{ process_null('aidsdoencaatualadenotamanho') }} AS NUMERIC) AS aidsdoencaatualadenotamanho,
        {{ process_null('aidsdoencaatualadenodolorosas') }} AS aidsdoencaatualadenodolorosas,
        {{ process_null('aidsdoencaatualvomitos') }} AS aidsdoencaatualvomitos,
        safe_cast({{ process_null('aidsdoencaatualdiarreia') }} AS NUMERIC) AS aidsdoencaatualdiarreia,
        {{ process_null('aidsdoencaatualdisfagia') }} AS aidsdoencaatualdisfagia,
        {{ process_null('aidsdoencaatualdorabdominal') }} AS aidsdoencaatualdorabdominal,
        {{ process_null('aidsdoencaatualtosse') }} AS aidsdoencaatualtosse,
        {{ process_null('aidsdoencaatualexpectoracao') }} AS aidsdoencaatualexpectoracao,
        {{ process_null('aidsdoencaatualdispneia') }} AS aidsdoencaatualdispneia,
        {{ process_null('aidsdoencaatualdortoraxica') }} AS aidsdoencaatualdortoraxica,
        {{ process_null('aidsdoencaatualcefaleias') }} AS aidsdoencaatualcefaleias,
        {{ process_null('aidsdoencaatualpertvisuais') }} AS aidsdoencaatualpertvisuais,
        {{ process_null('aidsdoencaatualaltcomportamento') }} AS aidsdoencaatualaltcomportamento,
        {{ process_null('aidsdoencaatualvertigens') }} AS aidsdoencaatualvertigens,
        {{ process_null('aidsdoencaatualaltesfincterianas') }} AS aidsdoencaatualaltesfincterianas,
        {{ process_null('aidsdoencaatualaltsensibilidade') }} AS aidsdoencaatualaltsensibilidade,
        {{ process_null('aidsdoencaatualaltmotorassup') }} AS aidsdoencaatualaltmotorassup,
        {{ process_null('aidsdoencaatualaltmotorasinf') }} AS aidsdoencaatualaltmotorasinf,
        safe_cast({{ process_null('aidsdoencaatualdtinicioqueixas') }} AS DATE) AS aidsdoencaatualdtinicioqueixas,
        {{ process_null('aidsdoencaatualobs') }} AS aidsdoencaatualobs,
        {{ process_null('aidstransfusaovertical') }} AS aidstransfusao_vertical,
        safe_cast({{ process_null('aidstransfusaoverticaldata') }} AS DATE) AS aidstransfusao_vertical_data,
        {{ process_null('aidshemofilico') }} AS aidshemofilico,
        safe_cast({{ process_null('aidshemofilicodata') }} AS DATE) AS aidshemofilicodata,
        {{ process_null('aidsacidentetrabalho') }} AS aidsacidentetrabalho,
        safe_cast({{ process_null('aidsacidentetrabalhodata') }} AS DATE) AS aidsacidentetrabalhodata,
        safe_cast({{ process_null('aidstempoemagrecimento') }} AS NUMERIC) AS aidstempoemagrecimento,
        {{ process_null('epidemiologiaignorado') }} AS epidemiologiaignorado,
        {{ process_null('aidsobsnotas') }} AS aidsobsnotas,
        {{ process_null('aidsemterapiaantiretroviral') }} AS aidsemterapiaantiretroviral,

        extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
    FROM hiv_deduplicados
)

SELECT
    *
FROM fato_hiv