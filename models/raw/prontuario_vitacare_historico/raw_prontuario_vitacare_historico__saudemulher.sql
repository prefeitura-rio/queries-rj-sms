{{
    config(
        alias="saude_mulher", 
        materialized="incremental",
        unique_key = 'id_prontuario_global',
        cluster_by= 'id_prontuario_global',
        schema="brutos_prontuario_vitacare_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

{% set last_partition = get_last_partition_date(this) %}

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
        {% if is_incremental() %}
            WHERE data_particao > '{{last_partition}}'
        {% endif %}
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

            {{ process_null('obspf') }} AS observacoes_planejamento_familiar,
            {{ process_null('sterilizationmethods') }} AS metodos_esterilizacao,
            {{ process_null('educationactions') }} AS acoes_educacao,
            {{ process_null('diudtainimcont') }} AS diu_data_inicio_mcont,
            {{ process_null('diudtatermomcont') }} AS diu_data_termino_mcont,
            {{ process_null('diuinterrmcont') }} AS diu_motivo_interrupcao_mcont,
            {{ process_null('diucompmcont') }} AS diu_complicacao_mcont,
            {{ process_null('chocqualmcont') }} AS choc_qual_mcont,
            {{ process_null('imqualmcont') }} AS im_qual_mcont,
            {{ process_null('itqualmcont') }} AS it_qual_mcont,
            {{ process_null('mpqualmcont') }} AS mp_qual_mcont,
            {{ process_null('oqualmcont') }} AS outro_qual_mcont,
            {{ process_null('pmdtainimcont') }} AS pm_data_inicio_mcont,
            {{ process_null('pfdtainimcont') }} AS pf_data_inicio_mcont,
            {{ process_null('chocdtainimcont') }} AS choc_data_inicio_mcont,
            {{ process_null('imdtainimcont') }} AS im_data_inicio_mcont,
            {{ process_null('itdtainimcont') }} AS it_data_inicio_mcont,
            {{ process_null('mpdtainimcont') }} AS mp_data_inicio_mcont,
            {{ process_null('dgdtainimcont') }} AS dg_data_inicio_mcont,
            {{ process_null('espdtainimcont') }} AS esp_data_inicio_mcont,
            {{ process_null('emdtainimcont') }} AS em_data_inicio_mcont,
            {{ process_null('efdtainimcont') }} AS ef_data_inicio_mcont,
            {{ process_null('odtainimcont') }} AS outro_data_inicio_mcont,
            {{ process_null('pmdtatermomcont') }} AS pm_data_termino_mcont,
            {{ process_null('pfdtatermomcont') }} AS pf_data_termino_mcont,
            {{ process_null('chocdtatermomcont') }} AS choc_data_termino_mcont,
            {{ process_null('imdtatermomcont') }} AS im_data_termino_mcont,
            {{ process_null('itdtatermomcont') }} AS it_data_termino_mcont,
            {{ process_null('mpdtatermomcont') }} AS mp_data_termino_mcont,
            {{ process_null('dgdtatermomcont') }} AS dg_data_termino_mcont,
            {{ process_null('espdtatermomcont') }} AS esp_data_termino_mcont,
            {{ process_null('emdtatermomcont') }} AS em_data_termino_mcont,
            {{ process_null('efdtatermomcont') }} AS ef_data_termino_mcont,
            {{ process_null('oddtatermomcont') }} AS outro_data_termino_mcont,
            {{ process_null('pminterrmcont') }} AS pm_motivo_interrupcao_mcont,
            {{ process_null('pfinterrmcont') }} AS pf_motivo_interrupcao_mcont,
            {{ process_null('chocinterrmcont') }} AS choc_motivo_interrupcao_mcont,
            {{ process_null('iminterrmcont') }} AS im_motivo_interrupcao_mcont,
            {{ process_null('itinterrmcont') }} AS it_motivo_interrupcao_mcont,
            {{ process_null('mpinterrmcont') }} AS mp_motivo_interrupcao_mcont,
            {{ process_null('dginterrmcont') }} AS dg_motivo_interrupcao_mcont,
            {{ process_null('espinterrmcont') }} AS esp_motivo_interrupcao_mcont,
            {{ process_null('eminterrmcont') }} AS em_motivo_interrupcao_mcont,
            {{ process_null('efinterrmcont') }} AS ef_motivo_interrupcao_mcont,
            {{ process_null('ointerrmcont') }} AS outro_motivo_interrupcao_mcont,
            {{ process_null('pmcompmcont') }} AS pm_complicacao_mcont,
            {{ process_null('pfcompmcont') }} AS pf_complicacao_mcont,
            {{ process_null('choccompmcont') }} AS choc_complicacao_mcont,
            {{ process_null('icompmcont') }} AS i_complicacao_mcont,
            {{ process_null('itcompmcont') }} AS it_complicacao_mcont,
            {{ process_null('mpcompmcont') }} AS mp_complicacao_mcont,
            {{ process_null('dgcompmcont') }} AS dg_complicacao_mcont,
            {{ process_null('espcompmcont') }} AS esp_complicacao_mcont,
            {{ process_null('emcompmcont') }} AS em_complicacao_mcont,
            {{ process_null('efcompmcont') }} AS ef_complicacao_mcont,
            {{ process_null('ocompmcont') }} AS o_complicacao_mcont,
            {{ process_null('diuqualmcont') }} AS diu_qual_mcont,
            {{ process_null('cidtainimcont') }} AS ci_data_inicio_mcont,
            {{ process_null('cidtatermomcont') }} AS ci_data_termino_mcont,
            {{ process_null('ciinterrmcont') }} AS ci_motivo_interrupcao_mcont,
            {{ process_null('cicompmcont') }} AS ci_complicacao_mcont,
            {{ process_null('tabdtainimcont') }} AS tab_data_inicio_mcont,
            {{ process_null('tabdtatermomcont') }} AS tab_data_termino_mcont,
            {{ process_null('tabinterrmcont') }} AS tab_motivo_interrupcao_mcont,
            {{ process_null('tabcompmcont') }} AS tab_complicacao_mcont,
            {{ process_null('mcontracepcaomcont') }} AS m_contracepcao_mcont,
            {{ process_null('mbdtainimcont') }} AS mb_data_inicio_mcont,
            {{ process_null('mbdtatermomcont') }} AS mb_data_termino_mcont,
            {{ process_null('mbinterrmcont') }} AS mb_motivo_interrupcao_mcont,
            {{ process_null('mbcompmcont') }} AS mb_complicacao_mcont,
            {{ process_null('cedtainimcont') }} AS ce_data_inicio_mcont,
            {{ process_null('cedtatermomcont') }} AS ce_data_termino_mcont,
            {{ process_null('ceinterrmcont') }} AS ce_motivo_interrupcao_mcont,
            {{ process_null('cecompmcont') }} AS ce_complicacao_mcont,
            {{ process_null('avdtainimcont') }} AS av_data_inicio_mcont,
            {{ process_null('avdtatermomcont') }} AS av_data_termino_mcont,
            {{ process_null('avinterrmcont') }} AS av_motivo_interrupcao_mcont,
            {{ process_null('avcompmcont') }} AS av_complicacao_mcont,
            {{ process_null('planondtainimcont') }} AS planon_data_inicio_mcont,
            {{ process_null('planondtatermomcont') }} AS planon_data_termino_mcont,
            {{ process_null('planonmotivomcont') }} AS planon_motivo_mcont,
            {{ process_null('planoncomplicacoesmcont') }} AS planon_complicacao_mcont,
   
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