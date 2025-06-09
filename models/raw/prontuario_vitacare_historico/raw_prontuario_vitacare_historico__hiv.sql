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
        FROM {{ source('brutos_prontuario_vitacare_historico', 'HIV') }} 
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

            {{ process_null(remove_double_quotes('aidstransfusao')) }} AS aidstransfusao,
            SAFE_CAST({{ process_null(remove_double_quotes('aidsdatatransfusao')) }} AS DATE) AS aidsdatatransfusao,
            {{ process_null(remove_double_quotes('aidsidadetransfusao')) }} AS aidsidadetransfusao,
            {{ process_null(remove_double_quotes('aidstoxicodependente')) }} AS aidstoxicodependente,
            SAFE_CAST({{ process_null(remove_double_quotes('aidsdatainiciotoxicodependente')) }} AS DATE) AS aidsdatainiciotoxicodependente,
            {{ process_null(remove_double_quotes('aidsidadeiniciotoxicodependente')) }} AS aidsidadeiniciotoxicodependente,
            {{ process_null(remove_double_quotes('aidshomosexual')) }} AS aidshomosexual,
            SAFE_CAST({{ process_null(remove_double_quotes('aidsdatainiciohomosexual')) }} AS DATE) AS aidsdatainiciohomosexual,
            {{ process_null(remove_double_quotes('aidsidadeiniciohomosexual')) }} AS aidsidadeiniciohomosexual,
            {{ process_null(remove_double_quotes('aidsheterosexual')) }} AS aidsheterosexual,
            SAFE_CAST({{ process_null(remove_double_quotes('aidsdatainicioheterosexual')) }} AS DATE) AS aidsdatainicioheterosexual,
            {{ process_null(remove_double_quotes('aidsidadeinicioheterosexual')) }} AS aidsidadeinicioheterosexual,
            {{ process_null(remove_double_quotes('aidsbisexual')) }} AS aidsbisexual,
            SAFE_CAST({{ process_null(remove_double_quotes('aidsdatainiciobisexual')) }} AS DATE) AS aidsdatainiciobisexual,
            {{ process_null(remove_double_quotes('aidsidadeiniciobisexual')) }} AS aidsidadeiniciobisexual,
            {{ process_null(remove_double_quotes('aidsparceirovih')) }} AS aidsparceirovih,
            SAFE_CAST({{ process_null(remove_double_quotes('aidsdatainicioparceirovih')) }} AS DATE) AS aidsdatainicioparceirovih,
            {{ process_null(remove_double_quotes('aidsidadeinicioparceirovih')) }} AS aidsidadeinicioparceirovih,
            {{ process_null(remove_double_quotes('aidsobsepidm')) }} AS aidsobsepidm,
            {{ process_null(remove_double_quotes('aidsdoencaatualano1teste')) }} AS aidsdoencaatualano1teste,
            SAFE_CAST({{ process_null(remove_double_quotes('aidsdoencaatualdtaprovavelinfec')) }} AS DATE) AS aidsdoencaatualdtaprovavelinfec,
            {{ process_null(remove_double_quotes('aidsdoencaatualfebre')) }} AS aidsdoencaatualfebre,
            {{ process_null(remove_double_quotes('aidsdoencaatualastenia')) }} AS aidsdoencaatualastenia,
            {{ process_null(remove_double_quotes('aidsdoencaatualanorexia')) }} AS aidsdoencaatualanorexia,
            {{ process_null(remove_double_quotes('aidsdoencaatualemagrecimento')) }} AS aidsdoencaatualemagrecimento,
            {{ process_null(remove_double_quotes('aidsdoencaatualhipersudorese')) }} AS aidsdoencaatualhipersudorese,
            {{ process_null(remove_double_quotes('aidsdoencaatualerupcaocutanea')) }} AS aidsdoencaatualerupcaocutanea,
            {{ process_null(remove_double_quotes('aidsdoencaatualaltpele')) }} AS aidsdoencaatualaltpele,
            {{ process_null(remove_double_quotes('aidsdoencaatualaltmucosa')) }} AS aidsdoencaatualaltmucosa,
            {{ process_null(remove_double_quotes('aidsdoencaatualadenopatias')) }} AS aidsdoencaatualadenopatias,
            {{ process_null(remove_double_quotes('aidsdoencaatualadenotamanho')) }} AS aidsdoencaatualadenotamanho,
            {{ process_null(remove_double_quotes('aidsdoencaatualadenodolorosas')) }} AS aidsdoencaatualadenodolorosas,
            {{ process_null(remove_double_quotes('aidsdoencaatualvomitos')) }} AS aidsdoencaatualvomitos,
            {{ process_null(remove_double_quotes('aidsdoencaatualdiarreia')) }} AS aidsdoencaatualdiarreia,
            {{ process_null(remove_double_quotes('aidsdoencaatualdisfagia')) }} AS aidsdoencaatualdisfagia,
            {{ process_null(remove_double_quotes('aidsdoencaatualdorabdominal')) }} AS aidsdoencaatualdorabdominal,
            {{ process_null(remove_double_quotes('aidsdoencaatualtosse')) }} AS aidsdoencaatualtosse,
            {{ process_null(remove_double_quotes('aidsdoencaatualexpectoracao')) }} AS aidsdoencaatualexpectoracao,
            {{ process_null(remove_double_quotes('aidsdoencaatualdispneia')) }} AS aidsdoencaatualdispneia,
            {{ process_null(remove_double_quotes('aidsdoencaatualdortoraxica')) }} AS aidsdoencaatualdortoraxica,
            {{ process_null(remove_double_quotes('aidsdoencaatualcefaleias')) }} AS aidsdoencaatualcefaleias,
            {{ process_null(remove_double_quotes('aidsdoencaatualpertvisuais')) }} AS aidsdoencaatualpertvisuais,
            {{ process_null(remove_double_quotes('aidsdoencaatualaltcomportamento')) }} AS aidsdoencaatualaltcomportamento,
            {{ process_null(remove_double_quotes('aidsdoencaatualvertigens')) }} AS aidsdoencaatualvertigens,
            {{ process_null(remove_double_quotes('aidsdoencaatualaltesfincterianas')) }} AS aidsdoencaatualaltesfincterianas,
            {{ process_null(remove_double_quotes('aidsdoencaatualaltsensibilidade')) }} AS aidsdoencaatualaltsensibilidade,
            {{ process_null(remove_double_quotes('aidsdoencaatualaltmotorassup')) }} AS aidsdoencaatualaltmotorassup,
            {{ process_null(remove_double_quotes('aidsdoencaatualaltmotorasinf')) }} AS aidsdoencaatualaltmotorasinf,
            SAFE_CAST({{ process_null(remove_double_quotes('aidsdoencaatualdtinicioqueixas')) }} AS DATE) AS aidsdoencaatualdtinicioqueixas,
            {{ process_null(remove_double_quotes('aidsdoencaatualobs')) }} AS aidsdoencaatualobs,
            {{ process_null(remove_double_quotes('aidstransfusaovertical')) }} AS aidstransfusaovertical,
            {{ process_null(remove_double_quotes('aidstransfusaoverticaldata')) }} AS aidstransfusaoverticaldata,
            {{ process_null(remove_double_quotes('aidshemofilico')) }} AS aidshemofilico,
            {{ process_null(remove_double_quotes('aidshemofilicodata')) }} AS aidshemofilicodata,
            {{ process_null(remove_double_quotes('aidsacidentetrabalho')) }} AS aidsacidentetrabalho,
            {{ process_null(remove_double_quotes('aidsacidentetrabalhodata')) }} AS aidsacidentetrabalhodata,
            {{ process_null(remove_double_quotes('aidstempoemagrecimento')) }} AS aidstempoemagrecimento,
            {{ process_null(remove_double_quotes('epidemiologiaignorado')) }} AS epidemiologiaignorado,
            {{ process_null(remove_double_quotes('aidsobsnotas')) }} AS aidsobsnotas,
            {{ process_null(remove_double_quotes('aidsemterapiaantiretroviral')) }} AS aidsemterapiaantiretroviral,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM hiv_deduplicados
    )

SELECT
    *
FROM fato_hiv