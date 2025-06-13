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
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'saudemulher') }} 
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

            {{ process_null(remove_double_quotes('obspf')) }} AS obspf,
            {{ process_null(remove_double_quotes('sterilizationmethods')) }} AS sterilizationmethods,
            {{ process_null(remove_double_quotes('educationactions')) }} AS educationactions,
            {{ process_null(remove_double_quotes('diudtainimcont')) }} AS diudtainimcont,
            {{ process_null(remove_double_quotes('diudtatermomcont')) }} AS diudtatermomcont,
            {{ process_null(remove_double_quotes('diuinterrmcont')) }} AS diuinterrmcont,
            {{ process_null(remove_double_quotes('diucompmcont')) }} AS diucompmcont,
            {{ process_null(remove_double_quotes('chocqualmcont')) }} AS chocqualmcont,
            {{ process_null(remove_double_quotes('imqualmcont')) }} AS imqualmcont,
            {{ process_null(remove_double_quotes('itqualmcont')) }} AS itqualmcont,
            {{ process_null(remove_double_quotes('mpqualmcont')) }} AS mpqualmcont,
            {{ process_null(remove_double_quotes('oqualmcont')) }} AS oqualmcont,
            {{ process_null(remove_double_quotes('pmdtainimcont')) }} AS pmdtainimcont,
            {{ process_null(remove_double_quotes('pfdtainimcont')) }} AS pfdtainimcont,
            {{ process_null(remove_double_quotes('chocdtainimcont')) }} AS chocdtainimcont,
            {{ process_null(remove_double_quotes('imdtainimcont')) }} AS imdtainimcont,
            {{ process_null(remove_double_quotes('itdtainimcont')) }} AS itdtainimcont,
            {{ process_null(remove_double_quotes('mpdtainimcont')) }} AS mpdtainimcont,
            {{ process_null(remove_double_quotes('dgdtainimcont')) }} AS dgdtainimcont,
            {{ process_null(remove_double_quotes('espdtainimcont')) }} AS espdtainimcont,
            {{ process_null(remove_double_quotes('emdtainimcont')) }} AS emdtainimcont,
            {{ process_null(remove_double_quotes('efdtainimcont')) }} AS efdtainimcont,
            {{ process_null(remove_double_quotes('odtainimcont')) }} AS odtainimcont,
            {{ process_null(remove_double_quotes('pmdtatermomcont')) }} AS pmdtatermomcont,
            {{ process_null(remove_double_quotes('pfdtatermomcont')) }} AS pfdtatermomcont,
            {{ process_null(remove_double_quotes('chocdtatermomcont')) }} AS chocdtatermomcont,
            {{ process_null(remove_double_quotes('imdtatermomcont')) }} AS imdtatermomcont,
            {{ process_null(remove_double_quotes('itdtatermomcont')) }} AS itdtatermomcont,
            {{ process_null(remove_double_quotes('mpdtatermomcont')) }} AS mpdtatermomcont,
            {{ process_null(remove_double_quotes('dgdtatermomcont')) }} AS dgdtatermomcont,
            {{ process_null(remove_double_quotes('espdtatermomcont')) }} AS espdtatermomcont,
            {{ process_null(remove_double_quotes('emdtatermomcont')) }} AS emdtatermomcont,
            {{ process_null(remove_double_quotes('efdtatermomcont')) }} AS efdtatermomcont,
            {{ process_null(remove_double_quotes('oddtatermomcont')) }} AS oddtatermomcont,
            {{ process_null(remove_double_quotes('pminterrmcont')) }} AS pminterrmcont,
            {{ process_null(remove_double_quotes('pfinterrmcont')) }} AS pfinterrmcont,
            {{ process_null(remove_double_quotes('chocinterrmcont')) }} AS chocinterrmcont,
            {{ process_null(remove_double_quotes('iminterrmcont')) }} AS iminterrmcont,
            {{ process_null(remove_double_quotes('itinterrmcont')) }} AS itinterrmcont,
            {{ process_null(remove_double_quotes('mpinterrmcont')) }} AS mpinterrmcont,
            {{ process_null(remove_double_quotes('dginterrmcont')) }} AS dginterrmcont,
            {{ process_null(remove_double_quotes('espinterrmcont')) }} AS espinterrmcont,
            {{ process_null(remove_double_quotes('eminterrmcont')) }} AS eminterrmcont,
            {{ process_null(remove_double_quotes('efinterrmcont')) }} AS efinterrmcont,
            {{ process_null(remove_double_quotes('ointerrmcont')) }} AS ointerrmcont,
            {{ process_null(remove_double_quotes('pmcompmcont')) }} AS pmcompmcont,
            {{ process_null(remove_double_quotes('pfcompmcont')) }} AS pfcompmcont,
            {{ process_null(remove_double_quotes('choccompmcont')) }} AS choccompmcont,
            {{ process_null(remove_double_quotes('icompmcont')) }} AS icompmcont,
            {{ process_null(remove_double_quotes('itcompmcont')) }} AS itcompmcont,
            {{ process_null(remove_double_quotes('mpcompmcont')) }} AS mpcompmcont,
            {{ process_null(remove_double_quotes('dgcompmcont')) }} AS dgcompmcont,
            {{ process_null(remove_double_quotes('espcompmcont')) }} AS espcompmcont,
            {{ process_null(remove_double_quotes('emcompmcont')) }} AS emcompmcont,
            {{ process_null(remove_double_quotes('efcompmcont')) }} AS efcompmcont,
            {{ process_null(remove_double_quotes('ocompmcont')) }} AS ocompmcont,
            {{ process_null(remove_double_quotes('diuqualmcont')) }} AS diuqualmcont,
            {{ process_null(remove_double_quotes('cidtainimcont')) }} AS cidtainimcont,
            {{ process_null(remove_double_quotes('cidtatermomcont')) }} AS cidtatermomcont,
            {{ process_null(remove_double_quotes('ciinterrmcont')) }} AS ciinterrmcont,
            {{ process_null(remove_double_quotes('cicompmcont')) }} AS cicompmcont,
            {{ process_null(remove_double_quotes('tabdtainimcont')) }} AS tabdtainimcont,
            {{ process_null(remove_double_quotes('tabdtatermomcont')) }} AS tabdtatermomcont,
            {{ process_null(remove_double_quotes('tabinterrmcont')) }} AS tabinterrmcont,
            {{ process_null(remove_double_quotes('tabcompmcont')) }} AS tabcompmcont,
            {{ process_null(remove_double_quotes('mcontracepcaomcont')) }} AS mcontracepcaomcont,
            {{ process_null(remove_double_quotes('mbdtainimcont')) }} AS mbdtainimcont,
            {{ process_null(remove_double_quotes('mbdtatermomcont')) }} AS mbdtatermomcont,
            {{ process_null(remove_double_quotes('mbinterrmcont')) }} AS mbinterrmcont,
            {{ process_null(remove_double_quotes('mbcompmcont')) }} AS mbcompmcont,
            {{ process_null(remove_double_quotes('cedtainimcont')) }} AS cedtainimcont,
            {{ process_null(remove_double_quotes('cedtatermomcont')) }} AS cedtatermomcont,
            {{ process_null(remove_double_quotes('ceinterrmcont')) }} AS ceinterrmcont,
            {{ process_null(remove_double_quotes('cecompmcont')) }} AS cecompmcont,
            {{ process_null(remove_double_quotes('avdtainimcont')) }} AS avdtainimcont,
            {{ process_null(remove_double_quotes('avdtatermomcont')) }} AS avdtatermomcont,
            {{ process_null(remove_double_quotes('avinterrmcont')) }} AS avinterrmcont,
            {{ process_null(remove_double_quotes('avcompmcont')) }} AS avcompmcont,
            {{ process_null(remove_double_quotes('planondtainimcont')) }} AS planondtainimcont,
            {{ process_null(remove_double_quotes('planondtatermomcont')) }} AS planondtatermomcont,
            {{ process_null(remove_double_quotes('planonmotivomcont')) }} AS planonmotivomcont,
            {{ process_null(remove_double_quotes('planoncomplicacoesmcont')) }} AS planoncomplicacoesmcont,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM saudemulher_deduplicados
    )

SELECT
    *
FROM fato_saudemulher