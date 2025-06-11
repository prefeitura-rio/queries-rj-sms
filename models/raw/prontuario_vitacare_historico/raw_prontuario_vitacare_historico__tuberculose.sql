{{
    config(
        alias="tuberculose", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_tuberculose AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
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
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ process_null(remove_double_quotes('datainiciotrat')) }} AS datainiciotrat,
            {{ process_null(remove_double_quotes('idadeiniciotrat')) }} AS idadeiniciotrat,
            {{ process_null(remove_double_quotes('pesoiniciotrat')) }} AS pesoiniciotrat,
            {{ process_null(remove_double_quotes('contatotb')) }} AS contatotb,
            {{ process_null(remove_double_quotes('tipoentrada')) }} AS tipoentrada,
            {{ process_null(remove_double_quotes('forma')) }} AS forma,
            {{ process_null(remove_double_quotes('baciloescarro')) }} AS baciloescarro,
            {{ process_null(remove_double_quotes('culturaescarro')) }} AS culturaescarro,
            {{ process_null(remove_double_quotes('formatratamento')) }} AS formatratamento,
            {{ process_null(remove_double_quotes('drogasutilizadas')) }} AS drogasutilizadas,
            {{ process_null(remove_double_quotes('tipoprograma')) }} AS tipoprograma,
            {{ process_null(remove_double_quotes('numsinan')) }} AS numsinan,
            {{ process_null(remove_double_quotes('hiv')) }} AS hiv,
            {{ process_null(remove_double_quotes('comunicatb')) }} AS comunicatb,
            {{ process_null(remove_double_quotes('raioxtorax')) }} AS raioxtorax,
            {{ process_null(remove_double_quotes('datainicioregistro')) }} AS datainicioregistro,
            {{ process_null(remove_double_quotes('resultadoraiox')) }} AS resultadoraiox,
            {{ process_null(remove_double_quotes('histbiopsiapleural')) }} AS histbiopsiapleural,
            {{ process_null(remove_double_quotes('histbiopsiaganglionar')) }} AS histbiopsiaganglionar,
            {{ process_null(remove_double_quotes('histoutrostecidos')) }} AS histoutrostecidos,
            {{ process_null(remove_double_quotes('sintomatologiarespiratoriaprevia')) }} AS sintomatologiarespiratoriaprevia,
            {{ process_null(remove_double_quotes('sintomatologiarespiratoriadiagnosticos')) }} AS sintomatologiarespiratoriadiagnosticos,
            {{ process_null(remove_double_quotes('testesensibilidadecultura')) }} AS testesensibilidadecultura,
            {{ process_null(remove_double_quotes('testemoleculartuberculose')) }} AS testemoleculartuberculose,
            {{ process_null(remove_double_quotes('igra')) }} AS igra,
            {{ process_null(remove_double_quotes('lflam')) }} AS lflam,
            {{ process_null(remove_double_quotes('lpa')) }} AS lpa,
            {{ process_null(remove_double_quotes('culturaoutros')) }} AS culturaoutros,
            {{ process_null(remove_double_quotes('culturaoutrosresultado')) }} AS culturaoutrosresultado,
            {{ process_null(remove_double_quotes('outrosexames')) }} AS outrosexames,
            {{ process_null(remove_double_quotes('pesoacompanhamentomensal')) }} AS pesoacompanhamentomensal,
            {{ process_null(remove_double_quotes('resultadobaciloescarro')) }} AS resultadobaciloescarro,
            {{ process_null(remove_double_quotes('fasetratamento')) }} AS fasetratamento,
            {{ process_null(remove_double_quotes('acompanhamentomensalobs')) }} AS acompanhamentomensalobs,
            {{ process_null(remove_double_quotes('hivpositivonegativomensal')) }} AS hivpositivonegativomensal,
            {{ process_null(remove_double_quotes('raioxtoraxmensal')) }} AS raioxtoraxmensal,
            {{ process_null(remove_double_quotes('resultadoraioxtoraxacompanhamentomensal')) }} AS resultadoraioxtoraxacompanhamentomensal,
            {{ process_null(remove_double_quotes('idadeexcltrat')) }} AS idadeexcltrat,
            {{ process_null(remove_double_quotes('dataexclusao')) }} AS dataexclusao,
            {{ process_null(remove_double_quotes('pesoencerramento')) }} AS pesoencerramento,
            {{ process_null(remove_double_quotes('resultadobaciloescarroencerramento')) }} AS resultadobaciloescarroencerramento,
            {{ process_null(remove_double_quotes('motivoencerramento')) }} AS motivoencerramento,
            {{ process_null(remove_double_quotes('hivpositivonegativoencerramento')) }} AS hivpositivonegativoencerramento,
            {{ process_null(remove_double_quotes('raioxtoraxencerramento')) }} AS raioxtoraxencerramento,
            {{ process_null(remove_double_quotes('resultadoraioxtoraxencerramento')) }} AS resultadoraioxtoraxencerramento,
            {{ process_null(remove_double_quotes('datainiciotratamentolatente')) }} AS datainiciotratamentolatente,
            {{ process_null(remove_double_quotes('raioxtoraxtratamentolatente')) }} AS raioxtoraxtratamentolatente,
            {{ process_null(remove_double_quotes('ppdtratamentolatente')) }} AS ppdtratamentolatente,
            {{ process_null(remove_double_quotes('ppddatatratamentolatente')) }} AS ppddatatratamentolatente,
            {{ process_null(remove_double_quotes('indicacaotratamentolatente')) }} AS indicacaotratamentolatente,
            {{ process_null(remove_double_quotes('dataencerramentotratamentolatente')) }} AS dataencerramentotratamentolatente,
            {{ process_null(remove_double_quotes('resultadoraioxtoraxtratamentolatente')) }} AS resultadoraioxtoraxtratamentolatente,
            {{ process_null(remove_double_quotes('esquemailtb')) }} AS esquemailtb,
            {{ process_null(remove_double_quotes('observacoestuber')) }} AS observacoestuber,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM tuberculose_deduplicados
    )

SELECT
    *
FROM fato_tuberculose