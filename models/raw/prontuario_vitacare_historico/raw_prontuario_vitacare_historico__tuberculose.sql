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
        FROM {{ source('brutos_vitacare_historic_staging', 'TUBERCULOSE') }} 
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

            {{ remove_double_quotes('datainiciotrat') }} AS datainiciotrat,
            {{ remove_double_quotes('idadeiniciotrat') }} AS idadeiniciotrat,
            {{ remove_double_quotes('pesoiniciotrat') }} AS pesoiniciotrat,
            {{ remove_double_quotes('contatotb') }} AS contatotb,
            {{ remove_double_quotes('tipoentrada') }} AS tipoentrada,
            {{ remove_double_quotes('forma') }} AS forma,
            {{ remove_double_quotes('baciloescarro') }} AS baciloescarro,
            {{ remove_double_quotes('culturaescarro') }} AS culturaescarro,
            {{ remove_double_quotes('formatratamento') }} AS formatratamento,
            {{ remove_double_quotes('drogasutilizadas') }} AS drogasutilizadas,
            {{ remove_double_quotes('tipoprograma') }} AS tipoprograma,
            {{ remove_double_quotes('numsinan') }} AS numsinan,
            {{ remove_double_quotes('hiv') }} AS hiv,
            {{ remove_double_quotes('comunicatb') }} AS comunicatb,
            {{ remove_double_quotes('raioxtorax') }} AS raioxtorax,
            {{ remove_double_quotes('datainicioregistro') }} AS datainicioregistro,
            {{ remove_double_quotes('resultadoraiox') }} AS resultadoraiox,
            {{ remove_double_quotes('histbiopsiapleural') }} AS histbiopsiapleural,
            {{ remove_double_quotes('histbiopsiaganglionar') }} AS histbiopsiaganglionar,
            {{ remove_double_quotes('histoutrostecidos') }} AS histoutrostecidos,
            {{ remove_double_quotes('sintomatologiarespiratoriaprevia') }} AS sintomatologiarespiratoriaprevia,
            {{ remove_double_quotes('sintomatologiarespiratoriadiagnosticos') }} AS sintomatologiarespiratoriadiagnosticos,
            {{ remove_double_quotes('testesensibilidadecultura') }} AS testesensibilidadecultura,
            {{ remove_double_quotes('testemoleculartuberculose') }} AS testemoleculartuberculose,
            {{ remove_double_quotes('igra') }} AS igra,
            {{ remove_double_quotes('lflam') }} AS lflam,
            {{ remove_double_quotes('lpa') }} AS lpa,
            {{ remove_double_quotes('culturaoutros') }} AS culturaoutros,
            {{ remove_double_quotes('culturaoutrosresultado') }} AS culturaoutrosresultado,
            {{ remove_double_quotes('outrosexames') }} AS outrosexames,
            {{ remove_double_quotes('pesoacompanhamentomensal') }} AS pesoacompanhamentomensal,
            {{ remove_double_quotes('resultadobaciloescarro') }} AS resultadobaciloescarro,
            {{ remove_double_quotes('fasetratamento') }} AS fasetratamento,
            {{ remove_double_quotes('acompanhamentomensalobs') }} AS acompanhamentomensalobs,
            {{ remove_double_quotes('hivpositivonegativomensal') }} AS hivpositivonegativomensal,
            {{ remove_double_quotes('raioxtoraxmensal') }} AS raioxtoraxmensal,
            {{ remove_double_quotes('resultadoraioxtoraxacompanhamentomensal') }} AS resultadoraioxtoraxacompanhamentomensal,
            {{ remove_double_quotes('idadeexcltrat') }} AS idadeexcltrat,
            {{ remove_double_quotes('dataexclusao') }} AS dataexclusao,
            {{ remove_double_quotes('pesoencerramento') }} AS pesoencerramento,
            {{ remove_double_quotes('resultadobaciloescarroencerramento') }} AS resultadobaciloescarroencerramento,
            {{ remove_double_quotes('motivoencerramento') }} AS motivoencerramento,
            {{ remove_double_quotes('hivpositivonegativoencerramento') }} AS hivpositivonegativoencerramento,
            {{ remove_double_quotes('raioxtoraxencerramento') }} AS raioxtoraxencerramento,
            {{ remove_double_quotes('resultadoraioxtoraxencerramento') }} AS resultadoraioxtoraxencerramento,
            {{ remove_double_quotes('datainiciotratamentolatente') }} AS datainiciotratamentolatente,
            {{ remove_double_quotes('raioxtoraxtratamentolatente') }} AS raioxtoraxtratamentolatente,
            {{ remove_double_quotes('ppdtratamentolatente') }} AS ppdtratamentolatente,
            {{ remove_double_quotes('ppddatatratamentolatente') }} AS ppddatatratamentolatente,
            {{ remove_double_quotes('indicacaotratamentolatente') }} AS indicacaotratamentolatente,
            {{ remove_double_quotes('dataencerramentotratamentolatente') }} AS dataencerramentotratamentolatente,
            {{ remove_double_quotes('resultadoraioxtoraxtratamentolatente') }} AS resultadoraioxtoraxtratamentolatente,
            {{ remove_double_quotes('esquemailtb') }} AS esquemailtb,
            {{ remove_double_quotes('observacoestuber') }} AS observacoestuber,

            {{ remove_double_quotes('extracted_at') }} AS extracted_at
            
        FROM tuberculose_deduplicados
    )

SELECT
    *
FROM fato_tuberculose