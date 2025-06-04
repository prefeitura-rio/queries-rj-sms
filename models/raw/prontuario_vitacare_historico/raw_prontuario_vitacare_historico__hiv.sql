{{
    config(
        alias="hiv", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
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
        FROM {{ source('brutos_vitacare_historic_staging', 'HIV') }} 
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
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ remove_double_quotes('aidstransfusao') }} AS aidstransfusao,
            SAFE_CAST({{ remove_double_quotes('aidsdatatransfusao') }} AS DATE) AS aidsdatatransfusao,
            {{ remove_double_quotes('aidsidadetransfusao') }} AS aidsidadetransfusao,
            {{ remove_double_quotes('aidstoxicodependente') }} AS aidstoxicodependente,
            SAFE_CAST({{ remove_double_quotes('aidsdatainiciotoxicodependente') }} AS DATE) AS aidsdatainiciotoxicodependente,
            {{ remove_double_quotes('aidsidadeiniciotoxicodependente') }} AS aidsidadeiniciotoxicodependente,
            {{ remove_double_quotes('aidshomosexual') }} AS aidshomosexual,
            SAFE_CAST({{ remove_double_quotes('aidsdatainiciohomosexual') }} AS DATE) AS aidsdatainiciohomosexual,
            {{ remove_double_quotes('aidsidadeiniciohomosexual') }} AS aidsidadeiniciohomosexual,
            {{ remove_double_quotes('aidsheterosexual') }} AS aidsheterosexual,
            SAFE_CAST({{ remove_double_quotes('aidsdatainicioheterosexual') }} AS DATE) AS aidsdatainicioheterosexual,
            {{ remove_double_quotes('aidsidadeinicioheterosexual') }} AS aidsidadeinicioheterosexual,
            {{ remove_double_quotes('aidsbisexual') }} AS aidsbisexual,
            SAFE_CAST({{ remove_double_quotes('aidsdatainiciobisexual') }} AS DATE) AS aidsdatainiciobisexual,
            {{ remove_double_quotes('aidsidadeiniciobisexual') }} AS aidsidadeiniciobisexual,
            {{ remove_double_quotes('aidsparceirovih') }} AS aidsparceirovih,
            SAFE_CAST{{ remove_double_quotes('aidsdatainicioparceirovih') }} AS DATE) AS aidsdatainicioparceirovih,
            {{ remove_double_quotes('aidsidadeinicioparceirovih') }} AS aidsidadeinicioparceirovih,
            {{ remove_double_quotes('aidsobsepidm') }} AS aidsobsepidm,
            {{ remove_double_quotes('aidsdoencaatualano1teste') }} AS aidsdoencaatualano1teste,
            SAFE_CAST({{ remove_double_quotes('aidsdoencaatualdtaprovavelinfec') }} AS DATE) AS aidsdoencaatualdtaprovavelinfec,
            {{ remove_double_quotes('aidsdoencaatualfebre') }} AS aidsdoencaatualfebre,
            {{ remove_double_quotes('aidsdoencaatualastenia') }} AS aidsdoencaatualastenia,
            {{ remove_double_quotes('aidsdoencaatualanorexia') }} AS aidsdoencaatualanorexia,
            {{ remove_double_quotes('aidsdoencaatualemagrecimento') }} AS aidsdoencaatualemagrecimento,
            {{ remove_double_quotes('aidsdoencaatualhipersudorese') }} AS aidsdoencaatualhipersudorese,
            {{ remove_double_quotes('aidsdoencaatualerupcaocutanea') }} AS aidsdoencaatualerupcaocutanea,
            {{ remove_double_quotes('aidsdoencaatualaltpele') }} AS aidsdoencaatualaltpele,
            {{ remove_double_quotes('aidsdoencaatualaltmucosa') }} AS aidsdoencaatualaltmucosa,
            {{ remove_double_quotes('aidsdoencaatualadenopatias') }} AS aidsdoencaatualadenopatias,
            {{ remove_double_quotes('aidsdoencaatualadenotamanho') }} AS aidsdoencaatualadenotamanho,
            {{ remove_double_quotes('aidsdoencaatualadenodolorosas') }} AS aidsdoencaatualadenodolorosas,
            {{ remove_double_quotes('aidsdoencaatualvomitos') }} AS aidsdoencaatualvomitos,
            {{ remove_double_quotes('aidsdoencaatualdiarreia') }} AS aidsdoencaatualdiarreia,
            {{ remove_double_quotes('aidsdoencaatualdisfagia') }} AS aidsdoencaatualdisfagia,
            {{ remove_double_quotes('aidsdoencaatualdorabdominal') }} AS aidsdoencaatualdorabdominal,
            {{ remove_double_quotes('aidsdoencaatualtosse') }} AS aidsdoencaatualtosse,
            {{ remove_double_quotes('aidsdoencaatualexpectoracao') }} AS aidsdoencaatualexpectoracao,
            {{ remove_double_quotes('aidsdoencaatualdispneia') }} AS aidsdoencaatualdispneia,
            {{ remove_double_quotes('aidsdoencaatualdortoraxica') }} AS aidsdoencaatualdortoraxica,
            {{ remove_double_quotes('aidsdoencaatualcefaleias') }} AS aidsdoencaatualcefaleias,
            {{ remove_double_quotes('aidsdoencaatualpertvisuais') }} AS aidsdoencaatualpertvisuais,
            {{ remove_double_quotes('aidsdoencaatualaltcomportamento') }} AS aidsdoencaatualaltcomportamento,
            {{ remove_double_quotes('aidsdoencaatualvertigens') }} AS aidsdoencaatualvertigens,
            {{ remove_double_quotes('aidsdoencaatualaltesfincterianas') }} AS aidsdoencaatualaltesfincterianas,
            {{ remove_double_quotes('aidsdoencaatualaltsensibilidade') }} AS aidsdoencaatualaltsensibilidade,
            {{ remove_double_quotes('aidsdoencaatualaltmotorassup') }} AS aidsdoencaatualaltmotorassup,
            {{ remove_double_quotes('aidsdoencaatualaltmotorasinf') }} AS aidsdoencaatualaltmotorasinf,
            SAFE_CAST({{ remove_double_quotes('aidsdoencaatualdtinicioqueixas') }} AS DATE) AS aidsdoencaatualdtinicioqueixas,
            {{ remove_double_quotes('aidsdoencaatualobs') }} AS aidsdoencaatualobs,
            {{ remove_double_quotes('aidstransfusaovertical') }} AS aidstransfusaovertical,
            {{ remove_double_quotes('aidstransfusaoverticaldata') }} AS aidstransfusaoverticaldata,
            {{ remove_double_quotes('aidshemofilico') }} AS aidshemofilico,
            {{ remove_double_quotes('aidshemofilicodata') }} AS aidshemofilicodata,
            {{ remove_double_quotes('aidsacidentetrabalho') }} AS aidsacidentetrabalho,
            {{ remove_double_quotes('aidsacidentetrabalhodata') }} AS aidsacidentetrabalhodata,
            {{ remove_double_quotes('aidstempoemagrecimento') }} AS aidstempoemagrecimento,
            {{ remove_double_quotes('epidemiologiaignorado') }} AS epidemiologiaignorado,
            {{ remove_double_quotes('aidsobsnotas') }} AS aidsobsnotas,
            {{ remove_double_quotes('aidsemterapiaantiretroviral') }} AS aidsemterapiaantiretroviral,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM hiv_deduplicados
    )

SELECT
    *
FROM fato_hiv