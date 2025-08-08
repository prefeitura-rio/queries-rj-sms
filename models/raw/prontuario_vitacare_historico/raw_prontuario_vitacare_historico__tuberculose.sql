{{
    config(
        alias="tuberculose", 
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

    source_tuberculose AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'tuberculose') }} 
    ),


      -- Using window function to deduplicate tuberculose
    tuberculose_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_tuberculose
        )
        WHERE rn = 1
    ),

    fato_tuberculose AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes,

            safe_cast({{ process_null('datainiciotrat') }} as DATETIME) AS data_inicio_trat,
            safe_cast({{ process_null('idadeiniciotrat') }} as INT) AS idade_inicio_trat,
            safe_cast({{ process_null('pesoiniciotrat') }} as NUMERIC) AS peso_inicio_trat,
            {{ process_null('contatotb') }} AS contato_tb,
            {{ process_null('tipoentrada') }} AS tipo_entrada,
            {{ process_null('forma') }} AS forma,
            {{ process_null('baciloescarro') }} AS bacilo_escarro,
            {{ process_null('culturaescarro') }} AS cultura_escarro,
            {{ process_null('formatratamento') }} AS forma_tratamento,
            {{ process_null('drogasutilizadas') }} AS drogas_utilizadas,
            {{ process_null('tipoprograma') }} AS tipo_programa,
            safe_cast({{ process_null('numsinan') }} as NUMERIC) AS num_sinan,
            {{ process_null('hiv') }} AS hiv,
            {{ process_null('comunicatb') }} AS comunica_tb,
            {{ process_null('raioxtorax') }} AS raiox_torax,
            safe_cast({{ process_null('datainicioregistro') }} as DATETIME) AS data_inicio_registro,
            {{ process_null('resultadoraiox') }} AS resultado_raiox,
            {{ process_null('histbiopsiapleural') }} AS hist_biopsia_pleural,
            {{ process_null('histbiopsiaganglionar') }} AS hist_biopsia_ganglionar,
            {{ process_null('histoutrostecidos') }} AS hist_outros_tecidos,
            {{ process_null('sintomatologiarespiratoriaprevia') }} AS sintomatologia_respiratoria_previa,
            {{ process_null('sintomatologiarespiratoriadiagnosticos') }} AS sintomatologia_respiratoria_diagnosticos,
            {{ process_null('testesensibilidadecultura') }} AS teste_sensibilidade_cultura,
            {{ process_null('testemoleculartuberculose') }} AS teste_molecular_tuberculose,
            {{ process_null('igra') }} AS igra,
            {{ process_null('lflam') }} AS lflam,
            {{ process_null('lpa') }} AS lpa,
            {{ process_null('culturaoutros') }} AS cultura_outros,
            {{ process_null('culturaoutrosresultado') }} AS cultura_outros_resultado,
            {{ process_null('outrosexames') }} AS outros_exames,
            safe_cast({{ process_null('pesoacompanhamentomensal') }} as NUMERIC) AS peso_acompanhamento_mensal,
            {{ process_null('resultadobaciloescarro') }} AS resultado_bacilo_escarro,
            {{ process_null('fasetratamento') }} AS fase_tratamento,
            {{ process_null('acompanhamentomensalobs') }} AS acompanhamento_mensal_obs,
            {{ process_null('hivpositivonegativomensal') }} AS hiv_positivo_negativo_mensal,
            {{ process_null('raioxtoraxmensal') }} AS raiox_torax_mensal,
            {{ process_null('resultadoraioxtoraxacompanhamentomensal') }} AS resultado_raiox_torax_acompanhamento_mensal,
            safe_cast({{ process_null('idadeexcltrat') }} as INT) AS idade_excl_trat,
            safe_cast({{ process_null('dataexclusao') }} as DATETIME) AS data_exclusao,
            safe_cast({{ process_null('pesoencerramento') }} as NUMERIC) AS peso_encerramento,
            {{ process_null('resultadobaciloescarroencerramento') }} AS resultado_bacilo_escarro_encerramento,
            {{ process_null('motivoencerramento') }} AS motivo_encerramento,
            {{ process_null('hivpositivonegativoencerramento') }} AS hiv_positivo_negativo_encerramento,
            {{ process_null('raioxtoraxencerramento') }} AS raiox_torax_encerramento,
            {{ process_null('resultadoraioxtoraxencerramento') }} AS resultado_raiox_torax_encerramento,
            safe_cast({{ process_null('datainiciotratamentolatente') }} as DATETIME) AS data_inicio_tratamento_latente,
            {{ process_null('raioxtoraxtratamentolatente') }} AS raiox_torax_tratamento_latente,
            {{ process_null('ppdtratamentolatente') }} AS ppd_tratamento_latente,
            safe_cast({{ process_null('ppddatatratamentolatente') }} as DATETIME) AS ppd_data_tratamento_latente,
            {{ process_null('indicacaotratamentolatente') }} AS indicacao_tratamento_latente,
            safe_cast({{ process_null('dataencerramentotratamentolatente') }} as DATETIME) AS data_encerramento_tratamento_latente,
            {{ process_null('resultadoraioxtoraxtratamentolatente') }} AS resultado_raiox_torax_tratamento_latente,
            {{ process_null('esquemailtb') }} AS esquema_iltb,
            {{ process_null('observacoestuber') }} AS observacoes_tuber,

            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
            
        FROM tuberculose_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_tuberculose
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado