{{
    config(
        alias="saudemulher", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_saudemulher AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_vitacare_historic_staging', 'SAUDEMULHER') }} 
    ),


      -- Using window function to deduplicate saudemulher
    saudemulher_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_saudemulher
        )
        WHERE rn = 1
    ),

    fato_saudemulher AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ remove_double_quotes('obspf') }} AS obspf,
            {{ remove_double_quotes('sterilizationmethods') }} AS sterilizationmethods,
            {{ remove_double_quotes('educationactions') }} AS educationactions,
            {{ remove_double_quotes('diudtainimcont') }} AS diudtainimcont,
            {{ remove_double_quotes('diudtatermomcont') }} AS diudtatermomcont,
            {{ remove_double_quotes('diuinterrmcont') }} AS diuinterrmcont,
            {{ remove_double_quotes('diucompmcont') }} AS diucompmcont,
            {{ remove_double_quotes('chocqualmcont') }} AS chocqualmcont,
            {{ remove_double_quotes('imqualmcont') }} AS imqualmcont,
            {{ remove_double_quotes('itqualmcont') }} AS itqualmcont,
            {{ remove_double_quotes('mpqualmcont') }} AS mpqualmcont,
            {{ remove_double_quotes('oqualmcont') }} AS oqualmcont,
            {{ remove_double_quotes('pmdtainimcont') }} AS pmdtainimcont,
            {{ remove_double_quotes('pfdtainimcont') }} AS pfdtainimcont,
            {{ remove_double_quotes('chocdtainimcont') }} AS chocdtainimcont,
            {{ remove_double_quotes('imdtainimcont') }} AS imdtainimcont,
            {{ remove_double_quotes('itdtainimcont') }} AS itdtainimcont,
            {{ remove_double_quotes('mpdtainimcont') }} AS mpdtainimcont,
            {{ remove_double_quotes('dgdtainimcont') }} AS dgdtainimcont,
            {{ remove_double_quotes('espdtainimcont') }} AS espdtainimcont,
            {{ remove_double_quotes('emdtainimcont') }} AS emdtainimcont,
            {{ remove_double_quotes('efdtainimcont') }} AS efdtainimcont,
            {{ remove_double_quotes('odtainimcont') }} AS odtainimcont,
            {{ remove_double_quotes('pmdtatermomcont') }} AS pmdtatermomcont,
            {{ remove_double_quotes('pfdtatermomcont') }} AS pfdtatermomcont,
            {{ remove_double_quotes('chocdtatermomcont') }} AS chocdtatermomcont,
            {{ remove_double_quotes('imdtatermomcont') }} AS imdtatermomcont,
            {{ remove_double_quotes('itdtatermomcont') }} AS itdtatermomcont,
            {{ remove_double_quotes('mpdtatermomcont') }} AS mpdtatermomcont,
            {{ remove_double_quotes('dgdtatermomcont') }} AS dgdtatermomcont,
            {{ remove_double_quotes('espdtatermomcont') }} AS espdtatermomcont,
            {{ remove_double_quotes('emdtatermomcont') }} AS emdtatermomcont,
            {{ remove_double_quotes('efdtatermomcont') }} AS efdtatermomcont,
            {{ remove_double_quotes('oddtatermomcont') }} AS oddtatermomcont,
            {{ remove_double_quotes('pminterrmcont') }} AS pminterrmcont,
            {{ remove_double_quotes('pfinterrmcont') }} AS pfinterrmcont,
            {{ remove_double_quotes('chocinterrmcont') }} AS chocinterrmcont,
            {{ remove_double_quotes('iminterrmcont') }} AS iminterrmcont,
            {{ remove_double_quotes('itinterrmcont') }} AS itinterrmcont,
            {{ remove_double_quotes('mpinterrmcont') }} AS mpinterrmcont,
            {{ remove_double_quotes('dginterrmcont') }} AS dginterrmcont,
            {{ remove_double_quotes('espinterrmcont') }} AS espinterrmcont,
            {{ remove_double_quotes('eminterrmcont') }} AS eminterrmcont,
            {{ remove_double_quotes('efinterrmcont') }} AS efinterrmcont,
            {{ remove_double_quotes('ointerrmcont') }} AS ointerrmcont,
            {{ remove_double_quotes('pmcompmcont') }} AS pmcompmcont,
            {{ remove_double_quotes('pfcompmcont') }} AS pfcompmcont,
            {{ remove_double_quotes('choccompmcont') }} AS choccompmcont,
            {{ remove_double_quotes('icompmcont') }} AS icompmcont,
            {{ remove_double_quotes('itcompmcont') }} AS itcompmcont,
            {{ remove_double_quotes('mpcompmcont') }} AS mpcompmcont,
            {{ remove_double_quotes('dgcompmcont') }} AS dgcompmcont,
            {{ remove_double_quotes('espcompmcont') }} AS espcompmcont,
            {{ remove_double_quotes('emcompmcont') }} AS emcompmcont,
            {{ remove_double_quotes('efcompmcont') }} AS efcompmcont,
            {{ remove_double_quotes('ocompmcont') }} AS ocompmcont,
            {{ remove_double_quotes('diuqualmcont') }} AS diuqualmcont,
            {{ remove_double_quotes('cidtainimcont') }} AS cidtainimcont,
            {{ remove_double_quotes('cidtatermomcont') }} AS cidtatermomcont,
            {{ remove_double_quotes('ciinterrmcont') }} AS ciinterrmcont,
            {{ remove_double_quotes('cicompmcont') }} AS cicompmcont,
            {{ remove_double_quotes('tabdtainimcont') }} AS tabdtainimcont,
            {{ remove_double_quotes('tabdtatermomcont') }} AS tabdtatermomcont,
            {{ remove_double_quotes('tabinterrmcont') }} AS tabinterrmcont,
            {{ remove_double_quotes('tabcompmcont') }} AS tabcompmcont,
            {{ remove_double_quotes('mcontracepcaomcont') }} AS mcontracepcaomcont,
            {{ remove_double_quotes('mbdtainimcont') }} AS mbdtainimcont,
            {{ remove_double_quotes('mbdtatermomcont') }} AS mbdtatermomcont,
            {{ remove_double_quotes('mbinterrmcont') }} AS mbinterrmcont,
            {{ remove_double_quotes('mbcompmcont') }} AS mbcompmcont,
            {{ remove_double_quotes('cedtainimcont') }} AS cedtainimcont,
            {{ remove_double_quotes('cedtatermomcont') }} AS cedtatermomcont,
            {{ remove_double_quotes('ceinterrmcont') }} AS ceinterrmcont,
            {{ remove_double_quotes('cecompmcont') }} AS cecompmcont,
            {{ remove_double_quotes('avdtainimcont') }} AS avdtainimcont,
            {{ remove_double_quotes('avdtatermomcont') }} AS avdtatermomcont,
            {{ remove_double_quotes('avinterrmcont') }} AS avinterrmcont,
            {{ remove_double_quotes('avcompmcont') }} AS avcompmcont,
            {{ remove_double_quotes('planondtainimcont') }} AS planondtainimcont,
            {{ remove_double_quotes('planondtatermomcont') }} AS planondtatermomcont,
            {{ remove_double_quotes('planonmotivomcont') }} AS planonmotivomcont,
            {{ remove_double_quotes('planoncomplicacoesmcont') }} AS planoncomplicacoesmcont,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM saudemulher_deduplicados
    )

SELECT
    *
FROM fato_saudemulher