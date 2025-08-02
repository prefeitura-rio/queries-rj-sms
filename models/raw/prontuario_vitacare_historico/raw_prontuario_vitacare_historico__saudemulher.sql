{{
    config(
        alias="saude_mulher", 
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

    source_saudemulher AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
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
            id_cnes,

            {{ process_null('obspf') }} AS obs_pf,
            {{ process_null('sterilizationmethods') }} AS sterilization_methods,
            {{ process_null('educationactions') }} AS education_actions,
            {{ process_null('diudtainimcont') }} AS diudtainim_cont,
            {{ process_null('diudtatermomcont') }} AS diudtatermom_cont,
            {{ process_null('diuinterrmcont') }} AS diuinterrm_cont,
            {{ process_null('diucompmcont') }} AS diucopm_cont,
            {{ process_null('chocqualmcont') }} AS chocqualm_cont,
            {{ process_null('imqualmcont') }} AS imqualm_cont,
            {{ process_null('itqualmcont') }} AS itqualm_cont,
            {{ process_null('mpqualmcont') }} AS mpqualm_cont,
            {{ process_null('oqualmcont') }} AS oqualm_cont,
            {{ process_null('pmdtainimcont') }} AS pmdtainim_cont,
            {{ process_null('pfdtainimcont') }} AS pfdtainim_cont,
            {{ process_null('chocdtainimcont') }} AS chocdtainim_cont,
            {{ process_null('imdtainimcont') }} AS imdtainim_cont,
            {{ process_null('itdtainimcont') }} AS itdtainim_cont,
            {{ process_null('mpdtainimcont') }} AS mpdtainim_cont,
            {{ process_null('dgdtainimcont') }} AS dgdtainim_cont,
            {{ process_null('espdtainimcont') }} AS espdtainim_cont,
            {{ process_null('emdtainimcont') }} AS emdtainim_cont,
            {{ process_null('efdtainimcont') }} AS efdtainim_cont,
            {{ process_null('odtainimcont') }} AS odtainim_cont,
            {{ process_null('pmdtatermomcont') }} AS pmdtatermom_cont,
            {{ process_null('pfdtatermomcont') }} AS pfdtatermom_cont,
            {{ process_null('chocdtatermomcont') }} AS chocdtatermom_cont,
            {{ process_null('imdtatermomcont') }} AS imdtatermom_cont,
            {{ process_null('itdtatermomcont') }} AS itdtatermom_cont,
            {{ process_null('mpdtatermomcont') }} AS mpdtatermom_cont,
            {{ process_null('dgdtatermomcont') }} AS dgdtatermom_cont,
            {{ process_null('espdtatermomcont') }} AS espdtatermom_cont,
            {{ process_null('emdtatermomcont') }} AS emdtatermom_cont,
            {{ process_null('efdtatermomcont') }} AS efdtatermom_cont,
            {{ process_null('oddtatermomcont') }} AS oddtatermom_cont,
            {{ process_null('pminterrmcont') }} AS pminterrm_cont,
            {{ process_null('pfinterrmcont') }} AS pfinterrm_cont,
            {{ process_null('chocinterrmcont') }} AS chocinterrm_cont,
            {{ process_null('iminterrmcont') }} AS iminterrm_cont,
            {{ process_null('itinterrmcont') }} AS itinterrm_cont,
            {{ process_null('mpinterrmcont') }} AS mpinterrm_cont,
            {{ process_null('dginterrmcont') }} AS dginterrm_cont,
            {{ process_null('espinterrmcont') }} AS espinterrm_cont,
            {{ process_null('eminterrmcont') }} AS eminterrm_cont,
            {{ process_null('efinterrmcont') }} AS efinterrm_cont,
            {{ process_null('ointerrmcont') }} AS ointerrm_cont,
            {{ process_null('pmcompmcont') }} AS pmcompm_cont,
            {{ process_null('pfcompmcont') }} AS pfcompm_cont,
            {{ process_null('choccompmcont') }} AS choccompm_cont,
            {{ process_null('icompmcont') }} AS icompm_cont,
            {{ process_null('itcompmcont') }} AS itcompm_cont,
            {{ process_null('mpcompmcont') }} AS mpcompm_cont,
            {{ process_null('dgcompmcont') }} AS dgcompm_cont,
            {{ process_null('espcompmcont') }} AS espcompm_cont,
            {{ process_null('emcompmcont') }} AS emcompm_cont,
            {{ process_null('efcompmcont') }} AS efcompm_cont,
            {{ process_null('ocompmcont') }} AS ocompm_cont,
            {{ process_null('diuqualmcont') }} AS diuqualm_cont,
            {{ process_null('cidtainimcont') }} AS cidtainim_cont,
            {{ process_null('cidtatermomcont') }} AS cidtatermom_cont,
            {{ process_null('ciinterrmcont') }} AS ciinterrm_cont,
            {{ process_null('cicompmcont') }} AS cicompm_cont,
            {{ process_null('tabdtainimcont') }} AS tabdtainim_cont,
            {{ process_null('tabdtatermomcont') }} AS tabdtatermom_cont,
            {{ process_null('tabinterrmcont') }} AS tabinterrm_cont,
            {{ process_null('tabcompmcont') }} AS tabcompm_cont,
            {{ process_null('mcontracepcaomcont') }} AS mcontracepcao_mcont,
            {{ process_null('mbdtainimcont') }} AS mbdtainim_cont,
            {{ process_null('mbdtatermomcont') }} AS mbdtatermom_cont,
            {{ process_null('mbinterrmcont') }} AS mbinterrm_cont,
            {{ process_null('mbcompmcont') }} AS mbcompm_cont,
            {{ process_null('cedtainimcont') }} AS cedtainim_cont,
            {{ process_null('cedtatermomcont') }} AS cedtatermom_cont,
            {{ process_null('ceinterrmcont') }} AS ceinterrm_cont,
            {{ process_null('cecompmcont') }} AS cecompm_cont,
            {{ process_null('avdtainimcont') }} AS avdtainim_cont,
            {{ process_null('avdtatermomcont') }} AS avdtatermom_cont,
            {{ process_null('avinterrmcont') }} AS avinterrm_cont,
            {{ process_null('avcompmcont') }} AS avcompm_cont,
            {{ process_null('planondtainimcont') }} AS planondtainim_cont,
            {{ process_null('planondtatermomcont') }} AS planondtatermom_cont,
            {{ process_null('planonmotivomcont') }} AS planonmotivo_mcont,
            {{ process_null('planoncomplicacoesmcont') }} AS planoncomplicacoes_mcont,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM saudemulher_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_saudemulher
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado