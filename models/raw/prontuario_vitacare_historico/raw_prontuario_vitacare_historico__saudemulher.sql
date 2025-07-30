{{
    config(
        alias="saude_mulher", 
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

            {{ process_null('obspf') }} AS obs_planejamento_familiar,
            {{ process_null('sterilizationmethods') }} AS metodos_esterilizacao,
            {{ process_null('educationactions') }} AS acoes_educacao,
            {{ process_null('diudtainimcont') }} AS diu_data_inicio_m_cont,
            {{ process_null('diudtatermomcont') }} AS diu_data_termino_m_cont,
            {{ process_null('diuinterrmcont') }} AS diu_interrupcao_motivo_m_cont,
            {{ process_null('diucompmcont') }} AS diu_complicacao_m_cont,
            {{ process_null('chocqualmcont') }} AS pilula_combinada_qual_m_cont,
            {{ process_null('imqualmcont') }} AS injecao_mensal_qual_m_cont,
            {{ process_null('itqualmcont') }} AS injecao_trimestral_qual_m_cont,
            {{ process_null('mpqualmcont') }} AS minipilula_qual_m_cont,
            {{ process_null('oqualmcont') }} AS outro_qual_m_cont,
            {{ process_null('pmdtainimcont') }} AS pilula_mensal_data_inicio_m_cont,
            {{ process_null('pfdtainimcont') }} AS pilula_familiar_data_inicio_m_cont,
            {{ process_null('chocdtainimcont') }} AS pilula_combinada_data_inicio_m_cont,
            {{ process_null('imdtainimcont') }} AS injecao_mensal_data_inicio_m_cont,
            {{ process_null('itdtainimcont') }} AS injecao_trimestral_data_inicio_m_cont,
            {{ process_null('mpdtainimcont') }} AS minipilula_data_inicio_m_cont,
            {{ process_null('dgdtainimcont') }} AS diafragma_data_inicio_m_cont,
            {{ process_null('espdtainimcont') }} AS espermicida_data_inicio_m_cont,
            {{ process_null('emdtainimcont') }} AS implante_data_inicio_m_cont,
            {{ process_null('efdtainimcont') }} AS adesivo_data_inicio_m_cont,
            {{ process_null('odtainimcont') }} AS outro_data_inicio_m_cont,
            {{ process_null('pmdtatermomcont') }} AS pilula_mensal_data_termino_m_cont,
            {{ process_null('pfdtatermomcont') }} AS pilula_familiar_data_termino_m_cont,
            {{ process_null('chocdtatermomcont') }} AS pilula_combinada_data_termino_m_cont,
            {{ process_null('imdtatermomcont') }} AS injecao_mensal_data_termino_m_cont,
            {{ process_null('itdtatermomcont') }} AS injecao_trimestral_data_termino_m_cont,
            {{ process_null('mpdtatermomcont') }} AS minipilula_data_termino_m_cont,
            {{ process_null('dgdtatermomcont') }} AS diafragma_data_termino_m_cont,
            {{ process_null('espdtatermomcont') }} AS espermicida_data_termino_m_cont,
            {{ process_null('emdtatermomcont') }} AS implante_data_termino_m_cont,
            {{ process_null('efdtatermomcont') }} AS adesivo_data_termino_m_cont,
            {{ process_null('oddtatermomcont') }} AS outro_data_termino_m_cont,
            {{ process_null('pminterrmcont') }} AS pilula_mensal_interrupcao_motivo_m_cont,
            {{ process_null('pfinterrmcont') }} AS pilula_familiar_interrupcao_motivo_m_cont,
            {{ process_null('chocinterrmcont') }} AS pilula_combinada_interrupcao_motivo_m_cont,
            {{ process_null('iminterrmcont') }} AS injecao_mensal_interrupcao_motivo_m_cont,
            {{ process_null('itinterrmcont') }} AS injecao_trimestral_interrupcao_motivo_m_cont,
            {{ process_null('mpinterrmcont') }} AS minipilula_interrupcao_motivo_m_cont,
            {{ process_null('dginterrmcont') }} AS diafragma_interrupcao_motivo_m_cont,
            {{ process_null('espinterrmcont') }} AS espermicida_interrupcao_motivo_m_cont,
            {{ process_null('eminterrmcont') }} AS implante_interrupcao_motivo_m_cont,
            {{ process_null('efinterrmcont') }} AS adesivo_interrupcao_motivo_m_cont,
            {{ process_null('ointerrmcont') }} AS outro_interrupcao_motivo_m_cont,
            {{ process_null('pmcompmcont') }} AS pilula_mensal_complicacao_m_cont,
            {{ process_null('pfcompmcont') }} AS pilula_familiar_complicacao_m_cont,
            {{ process_null('choccompmcont') }} AS pilula_combinada_complicacao_m_cont,
            {{ process_null('icompmcont') }} AS injecao_mensal_complicacao_m_cont,
            {{ process_null('itcompmcont') }} AS injecao_trimestral_complicacao_m_cont,
            {{ process_null('mpcompmcont') }} AS minipilula_complicacao_m_cont,
            {{ process_null('dgcompmcont') }} AS diafragma_complicacao_m_cont,
            {{ process_null('espcompmcont') }} AS espermicida_complicacao_m_cont,
            {{ process_null('emcompmcont') }} AS implante_complicacao_m_cont,
            {{ process_null('efcompmcont') }} AS adesivo_complicacao_m_cont,
            {{ process_null('ocompmcont') }} AS outro_complicacao_m_cont,
            {{ process_null('diuqualmcont') }} AS diu_qual_m_cont,
            {{ process_null('cidtainimcont') }} AS camisinha_masculina_data_inicio_m_cont,
            {{ process_null('cidtatermomcont') }} AS camisinha_masculina_data_termino_m_cont,
            {{ process_null('ciinterrmcont') }} AS camisinha_masculina_interrupcao_motivo_m_cont,
            {{ process_null('cicompmcont') }} AS camisinha_masculina_complicacao_m_cont,
            {{ process_null('tabdtainimcont') }} AS tabelinha_data_inicio_m_cont,
            {{ process_null('tabdtatermomcont') }} AS tabelinha_data_termino_m_cont,
            {{ process_null('tabinterrmcont') }} AS tabelinha_interrupcao_motivo_m_cont,
            {{ process_null('tabcompmcont') }} AS tabelinha_complicacao_m_cont,
            {{ process_null('mcontracepcaomcont') }} AS metodo_contracepcao_qual_m_cont,
            {{ process_null('mbdtainimcont') }} AS metodo_barreira_data_inicio_m_cont,
            {{ process_null('mbdtatermomcont') }} AS metodo_barreira_data_termino_m_cont,
            {{ process_null('mbinterrmcont') }} AS metodo_barreira_interrupcao_motivo_m_cont,
            {{ process_null('mbcompmcont') }} AS metodo_barreira_complicacao_m_cont,
            {{ process_null('cedtainimcont') }} AS camisinha_feminina_data_inicio_m_cont,
            {{ process_null('cedtatermomcont') }} AS camisinha_feminina_data_termino_m_cont,
            {{ process_null('ceinterrmcont') }} AS camisinha_feminina_interrupcao_motivo_m_cont,
            {{ process_null('cecompmcont') }} AS camisinha_feminina_complicacao_m_cont,
            {{ process_null('avdtainimcont') }} AS anel_vaginal_data_inicio_m_cont,
            {{ process_null('avdtatermomcont') }} AS anel_vaginal_data_termino_m_cont,
            {{ process_null('avinterrmcont') }} AS anel_vaginal_interrupcao_motivo_m_cont,
            {{ process_null('avcompmcont') }} AS anel_vaginal_complicacao_m_cont,
            {{ process_null('planondtainimcont') }} AS planejamento_natural_data_inicio_m_cont,
            {{ process_null('planondtatermomcont') }} AS planejamento_natural_data_termino_m_cont,
            {{ process_null('planonmotivomcont') }} AS planejamento_natural_motivo_m_cont,
            {{ process_null('planoncomplicacoesmcont') }} AS planejamento_natural_complicacoes_m_cont,

            extracted_at AS loaded_at
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