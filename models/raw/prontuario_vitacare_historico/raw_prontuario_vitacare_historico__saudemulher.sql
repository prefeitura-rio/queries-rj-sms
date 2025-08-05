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
            {{ process_null('diudtainimcont') }} AS diu_dtainim_mcont,
            {{ process_null('diudtatermomcont') }} AS diu_dtatermo_mcont,
            {{ process_null('diuinterrmcont') }} AS diu_interr_mcont,
            {{ process_null('diucompmcont') }} AS diu_comp_mcont,
            {{ process_null('chocqualmcont') }} AS choc_qual_mcont,
            {{ process_null('imqualmcont') }} AS im_qual_mcont,
            {{ process_null('itqualmcont') }} AS it_qual_mcont,
            {{ process_null('mpqualmcont') }} AS mp_qual_mcont,
            {{ process_null('oqualmcont') }} AS o_qual_mcont,
            {{ process_null('pmdtainimcont') }} AS pm_dtainim_mcont,
            {{ process_null('pfdtainimcont') }} AS pf_dtainim_mcont,
            {{ process_null('chocdtainimcont') }} AS choc_dtainim_mcont,
            {{ process_null('imdtainimcont') }} AS im_dtainim_mcont,
            {{ process_null('itdtainimcont') }} AS it_dtainim_mcont,
            {{ process_null('mpdtainimcont') }} AS mp_dtainim_mcont,
            {{ process_null('dgdtainimcont') }} AS dg_dtainim_mcont,
            {{ process_null('espdtainimcont') }} AS esp_dtainim_mcont,
            {{ process_null('emdtainimcont') }} AS em_dtainim_mcont,
            {{ process_null('efdtainimcont') }} AS ef_dtainim_mcont,
            {{ process_null('odtainimcont') }} AS o_dtainim_mcont,
            {{ process_null('pmdtatermomcont') }} AS pm_dtatermo_mcont,
            {{ process_null('pfdtatermomcont') }} AS pf_dtatermo_mcont,
            {{ process_null('chocdtatermomcont') }} AS choc_dtatermo_mcont,
            {{ process_null('imdtatermomcont') }} AS im_dtatermo_mcont,
            {{ process_null('itdtatermomcont') }} AS it_dtatermo_mcont,
            {{ process_null('mpdtatermomcont') }} AS mp_dtatermo_mcont,
            {{ process_null('dgdtatermomcont') }} AS dg_dtatermo_mcont,
            {{ process_null('espdtatermomcont') }} AS esp_dtatermo_mcont,
            {{ process_null('emdtatermomcont') }} AS em_dtatermo_mcont,
            {{ process_null('efdtatermomcont') }} AS ef_dtatermo_mcont,
            {{ process_null('oddtatermomcont') }} AS o_dtatermo_mcont,
            {{ process_null('pminterrmcont') }} AS pm_interr_mcont,
            {{ process_null('pfinterrmcont') }} AS pf_interr_mcont,
            {{ process_null('chocinterrmcont') }} AS choc_interr_mcont,
            {{ process_null('iminterrmcont') }} AS im_interr_mcont,
            {{ process_null('itinterrmcont') }} AS it_interr_mcont,
            {{ process_null('mpinterrmcont') }} AS mp_interr_mcont,
            {{ process_null('dginterrmcont') }} AS dg_interr_mcont,
            {{ process_null('espinterrmcont') }} AS esp_interr_mcont,
            {{ process_null('eminterrmcont') }} AS em_interr_mcont,
            {{ process_null('efinterrmcont') }} AS ef_interr_mcont,
            {{ process_null('ointerrmcont') }} AS o_interr_mcont,
            {{ process_null('pmcompmcont') }} AS pm_comp_mcont,
            {{ process_null('pfcompmcont') }} AS pf_comp_mcont,
            {{ process_null('choccompmcont') }} AS choc_comp_mcont,
            {{ process_null('icompmcont') }} AS i_comp_mcont,
            {{ process_null('itcompmcont') }} AS it_comp_mcont,
            {{ process_null('mpcompmcont') }} AS mp_comp_mcont,
            {{ process_null('dgcompmcont') }} AS dg_comp_mcont,
            {{ process_null('espcompmcont') }} AS esp_comp_mcont,
            {{ process_null('emcompmcont') }} AS em_comp_mcont,
            {{ process_null('efcompmcont') }} AS ef_comp_mcont,
            {{ process_null('ocompmcont') }} AS o_comp_mcont,
            {{ process_null('diuqualmcont') }} AS diu_qual_mcont,
            {{ process_null('cidtainimcont') }} AS ci_dtainim_mcont,
            {{ process_null('cidtatermomcont') }} AS ci_dtatermo_mcont,
            {{ process_null('ciinterrmcont') }} AS ci_interr_mcont,
            {{ process_null('cicompmcont') }} AS ci_comp_mcont,
            {{ process_null('tabdtainimcont') }} AS tab_dtainim_mcont,
            {{ process_null('tabdtatermomcont') }} AS tab_dtatermo_mcont,
            {{ process_null('tabinterrmcont') }} AS tab_interr_mcont,
            {{ process_null('tabcompmcont') }} AS tab_comp_mcont,
            {{ process_null('mcontracepcaomcont') }} AS m_contracepcao_mcont,
            {{ process_null('mbdtainimcont') }} AS mb_dtainim_mcont,
            {{ process_null('mbdtatermomcont') }} AS mb_dtatermo_mcont,
            {{ process_null('mbinterrmcont') }} AS mb_interr_mcont,
            {{ process_null('mbcompmcont') }} AS mb_comp_mcont,
            {{ process_null('cedtainimcont') }} AS ce_dtainim_mcont,
            {{ process_null('cedtatermomcont') }} AS ce_dtatermo_mcont,
            {{ process_null('ceinterrmcont') }} AS ce_interr_mcont,
            {{ process_null('cecompmcont') }} AS ce_comp_mcont,
            {{ process_null('avdtainimcont') }} AS av_dtainim_mcont,
            {{ process_null('avdtatermomcont') }} AS av_dtatermo_mcont,
            {{ process_null('avinterrmcont') }} AS av_interr_mcont,
            {{ process_null('avcompmcont') }} AS av_comp_mcont,
            {{ process_null('planondtainimcont') }} AS planon_dtainim_mcont,
            {{ process_null('planondtatermomcont') }} AS planon_dtatermo_mcont,
            {{ process_null('planonmotivomcont') }} AS planon_motivo_mcont,
            {{ process_null('planoncomplicacoesmcont') }} AS planon_complicacoes_mcont,
   
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