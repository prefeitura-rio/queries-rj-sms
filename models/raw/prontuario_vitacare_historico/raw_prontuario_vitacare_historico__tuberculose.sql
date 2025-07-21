{{
    config(
        alias="tuberculose", 
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

            safe_cast({{ process_null('datainiciotrat') }} as DATETIME) AS datainiciotrat,
            safe_cast({{ process_null('idadeiniciotrat') }} as INT) AS idadeiniciotrat,
            safe_cast({{ process_null('pesoiniciotrat') }} as NUMERIC) AS pesoiniciotrat,
            {{ process_null('contatotb') }} AS contatotb,
            {{ process_null('tipoentrada') }} AS tipoentrada,
            {{ process_null('forma') }} AS forma,
            {{ process_null('baciloescarro') }} AS baciloescarro,
            {{ process_null('culturaescarro') }} AS culturaescarro,
            {{ process_null('formatratamento') }} AS formatratamento,
            {{ process_null('drogasutilizadas') }} AS drogasutilizadas,
            {{ process_null('tipoprograma') }} AS tipoprograma,
            safe_cast({{ process_null('numsinan') }} as NUMERIC) AS numsinan,
            {{ process_null('hiv') }} AS hiv,
            {{ process_null('comunicatb') }} AS comunicatb,
            {{ process_null('raioxtorax') }} AS raioxtorax,
            safe_cast({{ process_null('datainicioregistro') }} as DATETIME) AS datainicioregistro,
            {{ process_null('resultadoraiox') }} AS resultadoraiox,
            {{ process_null('histbiopsiapleural') }} AS histbiopsiapleural,
            {{ process_null('histbiopsiaganglionar') }} AS histbiopsiaganglionar,
            {{ process_null('histoutrostecidos') }} AS histoutrostecidos,
            {{ process_null('sintomatologiarespiratoriaprevia') }} AS sintomatologiarespiratoriaprevia,
            {{ process_null('sintomatologiarespiratoriadiagnosticos') }} AS sintomatologiarespiratoriadiagnosticos,
            {{ process_null('testesensibilidadecultura') }} AS testesensibilidadecultura,
            {{ process_null('testemoleculartuberculose') }} AS testemoleculartuberculose,
            {{ process_null('igra') }} AS igra,
            {{ process_null('lflam') }} AS lflam,
            {{ process_null('lpa') }} AS lpa,
            {{ process_null('culturaoutros') }} AS culturaoutros,
            {{ process_null('culturaoutrosresultado') }} AS culturaoutrosresultado,
            {{ process_null('outrosexames') }} AS outrosexames,
            safe_cast({{ process_null('pesoacompanhamentomensal') }} as NUMERIC) AS pesoacompanhamentomensal,
            {{ process_null('resultadobaciloescarro') }} AS resultadobaciloescarro,
            {{ process_null('fasetratamento') }} AS fasetratamento,
            {{ process_null('acompanhamentomensalobs') }} AS acompanhamentomensalobs,
            {{ process_null('hivpositivonegativomensal') }} AS hivpositivonegativomensal,
            {{ process_null('raioxtoraxmensal') }} AS raioxtoraxmensal,
            {{ process_null('resultadoraioxtoraxacompanhamentomensal') }} AS resultadoraioxtoraxacompanhamentomensal,
            safe_cast({{ process_null('idadeexcltrat') }} as INT) AS idadeexcltrat,
            safe_cast({{ process_null('dataexclusao') }} as DATETIME) AS dataexclusao,
            safe_cast({{ process_null('pesoencerramento') }} as NUMERIC) AS pesoencerramento,
            {{ process_null('resultadobaciloescarroencerramento') }} AS resultadobaciloescarroencerramento,
            {{ process_null('motivoencerramento') }} AS motivoencerramento,
            {{ process_null('hivpositivonegativoencerramento') }} AS hivpositivonegativoencerramento,
            {{ process_null('raioxtoraxencerramento') }} AS raioxtoraxencerramento,
            {{ process_null('resultadoraioxtoraxencerramento') }} AS resultadoraioxtoraxencerramento,
            safe_cast({{ process_null('datainiciotratamentolatente') }} as DATETIME) AS datainiciotratamentolatente,
            {{ process_null('raioxtoraxtratamentolatente') }} AS raioxtoraxtratamentolatente,
            {{ process_null('ppdtratamentolatente') }} AS ppdtratamentolatente,
            safe_cast({{ process_null('ppddatatratamentolatente') }} as DATETIME) AS ppddatatratamentolatente,
            {{ process_null('indicacaotratamentolatente') }} AS indicacaotratamentolatente,
            safe_cast({{ process_null('dataencerramentotratamentolatente') }} as DATETIME) AS dataencerramentotratamentolatente,
            {{ process_null('resultadoraioxtoraxtratamentolatente') }} AS resultadoraioxtoraxtratamentolatente,
            {{ process_null('esquemailtb') }} AS esquemailtb,
            {{ process_null('observacoestuber') }} AS observacoestuber,

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