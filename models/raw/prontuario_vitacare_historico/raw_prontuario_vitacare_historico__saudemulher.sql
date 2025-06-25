{{
    config(
        alias="saudemulher", 
        materialized="incremental",
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
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
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes AS cnes_unidade,

            {{ process_null('obspf') }} AS obspf,
            {{ process_null('sterilizationmethods') }} AS sterilizationmethods,
            {{ process_null('educationactions') }} AS educationactions,
            {{ process_null('diudtainimcont') }} AS diudtainimcont,
            {{ process_null('diudtatermomcont') }} AS diudtatermomcont,
            {{ process_null('diuinterrmcont') }} AS diuinterrmcont,
            {{ process_null('diucompmcont') }} AS diucompmcont,
            {{ process_null('chocqualmcont') }} AS chocqualmcont,
            {{ process_null('imqualmcont') }} AS imqualmcont,
            {{ process_null('itqualmcont') }} AS itqualmcont,
            {{ process_null('mpqualmcont') }} AS mpqualmcont,
            {{ process_null('oqualmcont') }} AS oqualmcont,
            {{ process_null('pmdtainimcont') }} AS pmdtainimcont,
            {{ process_null('pfdtainimcont') }} AS pfdtainimcont,
            {{ process_null('chocdtainimcont') }} AS chocdtainimcont,
            {{ process_null('imdtainimcont') }} AS imdtainimcont,
            {{ process_null('itdtainimcont') }} AS itdtainimcont,
            {{ process_null('mpdtainimcont') }} AS mpdtainimcont,
            {{ process_null('dgdtainimcont') }} AS dgdtainimcont,
            {{ process_null('espdtainimcont') }} AS espdtainimcont,
            {{ process_null('emdtainimcont') }} AS emdtainimcont,
            {{ process_null('efdtainimcont') }} AS efdtainimcont,
            {{ process_null('odtainimcont') }} AS odtainimcont,
            {{ process_null('pmdtatermomcont') }} AS pmdtatermomcont,
            {{ process_null('pfdtatermomcont') }} AS pfdtatermomcont,
            {{ process_null('chocdtatermomcont') }} AS chocdtatermomcont,
            {{ process_null('imdtatermomcont') }} AS imdtatermomcont,
            {{ process_null('itdtatermomcont') }} AS itdtatermomcont,
            {{ process_null('mpdtatermomcont') }} AS mpdtatermomcont,
            {{ process_null('dgdtatermomcont') }} AS dgdtatermomcont,
            {{ process_null('espdtatermomcont') }} AS espdtatermomcont,
            {{ process_null('emdtatermomcont') }} AS emdtatermomcont,
            {{ process_null('efdtatermomcont') }} AS efdtatermomcont,
            {{ process_null('oddtatermomcont') }} AS oddtatermomcont,
            {{ process_null('pminterrmcont') }} AS pminterrmcont,
            {{ process_null('pfinterrmcont') }} AS pfinterrmcont,
            {{ process_null('chocinterrmcont') }} AS chocinterrmcont,
            {{ process_null('iminterrmcont') }} AS iminterrmcont,
            {{ process_null('itinterrmcont') }} AS itinterrmcont,
            {{ process_null('mpinterrmcont') }} AS mpinterrmcont,
            {{ process_null('dginterrmcont') }} AS dginterrmcont,
            {{ process_null('espinterrmcont') }} AS espinterrmcont,
            {{ process_null('eminterrmcont') }} AS eminterrmcont,
            {{ process_null('efinterrmcont') }} AS efinterrmcont,
            {{ process_null('ointerrmcont') }} AS ointerrmcont,
            {{ process_null('pmcompmcont') }} AS pmcompmcont,
            {{ process_null('pfcompmcont') }} AS pfcompmcont,
            {{ process_null('choccompmcont') }} AS choccompmcont,
            {{ process_null('icompmcont') }} AS icompmcont,
            {{ process_null('itcompmcont') }} AS itcompmcont,
            {{ process_null('mpcompmcont') }} AS mpcompmcont,
            {{ process_null('dgcompmcont') }} AS dgcompmcont,
            {{ process_null('espcompmcont') }} AS espcompmcont,
            {{ process_null('emcompmcont') }} AS emcompmcont,
            {{ process_null('efcompmcont') }} AS efcompmcont,
            {{ process_null('ocompmcont') }} AS ocompmcont,
            {{ process_null('diuqualmcont') }} AS diuqualmcont,
            {{ process_null('cidtainimcont') }} AS cidtainimcont,
            {{ process_null('cidtatermomcont') }} AS cidtatermomcont,
            {{ process_null('ciinterrmcont') }} AS ciinterrmcont,
            {{ process_null('cicompmcont') }} AS cicompmcont,
            {{ process_null('tabdtainimcont') }} AS tabdtainimcont,
            {{ process_null('tabdtatermomcont') }} AS tabdtatermomcont,
            {{ process_null('tabinterrmcont') }} AS tabinterrmcont,
            {{ process_null('tabcompmcont') }} AS tabcompmcont,
            {{ process_null('mcontracepcaomcont') }} AS mcontracepcaomcont,
            {{ process_null('mbdtainimcont') }} AS mbdtainimcont,
            {{ process_null('mbdtatermomcont') }} AS mbdtatermomcont,
            {{ process_null('mbinterrmcont') }} AS mbinterrmcont,
            {{ process_null('mbcompmcont') }} AS mbcompmcont,
            {{ process_null('cedtainimcont') }} AS cedtainimcont,
            {{ process_null('cedtatermomcont') }} AS cedtatermomcont,
            {{ process_null('ceinterrmcont') }} AS ceinterrmcont,
            {{ process_null('cecompmcont') }} AS cecompmcont,
            {{ process_null('avdtainimcont') }} AS avdtainimcont,
            {{ process_null('avdtatermomcont') }} AS avdtatermomcont,
            {{ process_null('avinterrmcont') }} AS avinterrmcont,
            {{ process_null('avcompmcont') }} AS avcompmcont,
            {{ process_null('planondtainimcont') }} AS planondtainimcont,
            {{ process_null('planondtatermomcont') }} AS planondtatermomcont,
            {{ process_null('planonmotivomcont') }} AS planonmotivomcont,
            {{ process_null('planoncomplicacoesmcont') }} AS planoncomplicacoesmcont,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM saudemulher_deduplicados
    )

SELECT
    *
FROM fato_saudemulher