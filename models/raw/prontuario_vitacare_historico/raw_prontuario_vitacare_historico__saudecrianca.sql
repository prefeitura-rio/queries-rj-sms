{{
    config(
        alias="saudecrianca", 
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

    source_saudecrianca AS (
        SELECT 
            CONCAT(
                NULLIF(id_cnes, ''), 
                '.',
                NULLIF(REPLACE(acto_id, '.0', ''), '')
            ) AS id_prontuario_global,
            *
        FROM {{ source('brutos_prontuario_vitacare_historico_staging', 'saudecrianca') }} 
    ),


      -- Using window function to deduplicate saudecrianca
    saudecrianca_deduplicados AS (
        SELECT
            *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY extracted_at DESC) AS rn
            FROM source_saudecrianca
        )
        WHERE rn = 1
    ),

    fato_saudecrianca AS (
        SELECT
            -- PKs e Chaves
            id_prontuario_global,
            REPLACE(acto_id, '.0', '') AS id_prontuario_local,
            id_cnes ,

            {{ process_null('ppneonatal') }} AS ppneonatal,
            {{ process_null('ppneonatalobs') }} AS ppneonatalobs,
            {{ process_null('anomaliascongenitas') }} AS anomaliascongenitas,
            SAFE_CAST({{ process_null('regcrescaltura') }} AS NUMERIC) AS regcrescaltura,
            SAFE_CAST({{ process_null('regcrescpeso') }} AS NUMERIC) AS regcrescpeso,
            {{ process_null('observacoes') }} AS observacoes,
            SAFE_CAST({{ process_null('regcrescimc') }} AS NUMERIC) AS regcrescimc,
            {{ process_null('rastreios') }} AS rastreios,
            {{ process_null('anamaliascongobs') }} AS anamaliascongobs,
            {{ process_null('registocrescimentioobs') }} AS registocrescimentioobs,
            {{ process_null('tipoparto') }} AS tipoparto,
            SAFE_CAST({{ process_null('pesonascer') }} AS NUMERIC) AS pesonascer,
            SAFE_CAST({{ process_null('comprimento') }} AS NUMERIC) AS comprimento,
            SAFE_CAST({{ process_null('perimetrocefalico') }} AS NUMERIC) AS perimetrocefalico,
            SAFE_CAST({{ process_null('indiceapgar1minuto') }} AS NUMERIC) AS indiceapgar1minuto,
            SAFE_CAST({{ process_null('indiceapgar5minuto') }} AS NUMERIC) AS indiceapgar5minuto,
            {{ process_null('reanimacao') }} AS reanimacao,
            {{ process_null('exameocularqueixas') }} AS exameocularqueixas,
            {{ process_null('exameoculardoencas') }} AS exameoculardoencas,
            {{ process_null('exameocularnecessitaoculos') }} AS exameocularnecessitaoculos,
            {{ process_null('exameocularreferencia') }} AS exameocularreferencia,
            {{ process_null('exameocularestado') }} AS exameocularestado,
            {{ process_null('exameocularobservacoes') }} AS exameocularobservacoes,
            {{ process_null('testepezinho') }} AS testepezinho,
            {{ process_null('testepezinhodoencas') }} AS testepezinhodoencas,
            {{ process_null('reflexovermelho') }} AS reflexovermelho,
            {{ process_null('testeorelhinha') }} AS testeorelhinha,
            {{ process_null('periodoprenatal') }} AS periodoprenatal,
            {{ process_null('denverperiodotemporal') }} AS denverperiodotemporal,
            {{ process_null('denverpostura') }} AS denverpostura,
            {{ process_null('denverobservarosto') }} AS denverobservarosto,
            {{ process_null('denverreagesom') }} AS denverreagesom,
            {{ process_null('denverelevacabeca') }} AS denverelevacabeca,
            {{ process_null('denversorriso') }} AS denversorriso,
            {{ process_null('denverabremaos') }} AS denverabremaos,
            {{ process_null('denveremitesons') }} AS denveremitesons,
            {{ process_null('denvermovimentos') }} AS denvermovimentos,
            {{ process_null('denvercontatosocial') }} AS denvercontatosocial,
            {{ process_null('denverseguraobjetos') }} AS denverseguraobjetos,
            {{ process_null('denveremitesons24meses') }} AS denveremitesons24meses,
            {{ process_null('denverbrucolevantacabeca') }} AS denverbrucolevantacabeca,
            {{ process_null('denverbuscaobjectos') }} AS denverbuscaobjectos,
            {{ process_null('denverlevaobjectos') }} AS denverlevaobjectos,
            {{ process_null('denverlocalizasom') }} AS denverlocalizasom,
            {{ process_null('denvermudaposicaorola') }} AS denvermudaposicaorola,
            {{ process_null('denverbrincaescondeachou') }} AS denverbrincaescondeachou,
            {{ process_null('denvertransfereobjectos') }} AS denvertransfereobjectos,
            {{ process_null('denverduplicasilabas') }} AS denverduplicasilabas,
            {{ process_null('denversentasemapoio') }} AS denversentasemapoio,
            {{ process_null('denverimitagestos') }} AS denverimitagestos,
            {{ process_null('denverfazpinca') }} AS denverfazpinca,
            {{ process_null('denverproduzjargao') }} AS denverproduzjargao,
            {{ process_null('denverandacomapoio') }} AS denverandacomapoio,
            {{ process_null('denvermostraroquequer') }} AS denvermostraroquequer,
            {{ process_null('denvercolocablocoscaneca') }} AS denvercolocablocoscaneca,
            {{ process_null('denverdizumapalavra') }} AS denverdizumapalavra,
            {{ process_null('denverandaparatras') }} AS denverandaparatras,
            {{ process_null('denvertiraroupa') }} AS denvertiraroupa,
            {{ process_null('denverconstroitorre3cubos') }} AS denverconstroitorre3cubos,
            {{ process_null('denveraponta2figuras') }} AS denveraponta2figuras,
            {{ process_null('denverchutabola') }} AS denverchutabola,
            {{ process_null('denvervestecomsupervisao') }} AS denvervestecomsupervisao,
            {{ process_null('denverconstroitorre6cubos') }} AS denverconstroitorre6cubos,
            {{ process_null('denverfrases2palavras') }} AS denverfrases2palavras,
            {{ process_null('denverpulaambospes') }} AS denverpulaambospes,
            {{ process_null('denverbrincacomoutrascriancas') }} AS denverbrincacomoutrascriancas,
            {{ process_null('denverimitalinhavertical') }} AS denverimitalinhavertical,
            {{ process_null('denverreconhece2acoes') }} AS denverreconhece2acoes,
            {{ process_null('denverarremessabola') }} AS denverarremessabola,
            {{ process_null('rastreiostestepezinhoobservacoes') }} AS rastreiostestepezinhoobservacoes,
            {{ process_null('rastreiosreflexovermelhoobservacoes') }} AS rastreiosreflexovermelhoobservacoes,
            {{ process_null('rastreiostesteorelhinhaobservacoes') }} AS rastreiostesteorelhinhaobservacoes,
            {{ process_null('tecnicasespeciais') }} AS tecnicasespeciais,
            {{ process_null('tecnicasespeciaisoutras') }} AS tecnicasespeciaisoutras,
            {{ process_null('sintomasacuidadevisual') }} AS sintomasacuidadevisual,
            {{ process_null('snellendistanciaesq') }} AS snellendistanciaesq,
            {{ process_null('snellendistanciadir') }} AS snellendistanciadir,
            {{ process_null('snellentipooptotiposesq') }} AS snellentipooptotiposesq,
            {{ process_null('snellentipooptotiposdir') }} AS snellentipooptotiposdir,
            SAFE_CAST({{ process_null('comcorrecaoesq') }} AS NUMERIC) AS comcorrecaoesq,
            SAFE_CAST({{ process_null('semcorrecaoesq') }} AS NUMERIC) AS semcorrecaoesq,
            SAFE_CAST({{ process_null('comcorrecaodir') }} AS NUMERIC) AS comcorrecaodir,
            SAFE_CAST({{ process_null('semcorrecaodir') }} AS NUMERIC) AS semcorrecaodir,
            {{ process_null('acuidadevisualconduta') }} AS acuidadevisualconduta,
            {{ process_null('triagemautismoolhaobjeto') }} AS triagemautismoolhaobjeto,
            {{ process_null('triagemautismopodesersurda') }} AS triagemautismopodesersurda,
            {{ process_null('triagemautismobrincafazdecontas') }} AS triagemautismobrincafazdecontas,
            {{ process_null('triagemautismosubirnascoisas') }} AS triagemautismosubirnascoisas,
            {{ process_null('triagemautismomovimentosestranhos') }} AS triagemautismomovimentosestranhos,
            {{ process_null('triagemautismoapontaodedopedir') }} AS triagemautismoapontaodedopedir,
            {{ process_null('triagemautismoapontaodedomostrar') }} AS triagemautismoapontaodedomostrar,
            {{ process_null('triagemautismointeresseoutrascriancas') }} AS triagemautismointeresseoutrascriancas,
            {{ process_null('triagemautismotrazcoisas') }} AS triagemautismotrazcoisas,
            {{ process_null('triagemautismorespondepelonome') }} AS triagemautismorespondepelonome,
            {{ process_null('triagemautismosorri') }} AS triagemautismosorri,
            {{ process_null('triagemautismoincomodadabarulho') }} AS triagemautismoincomodadabarulho,
            {{ process_null('triagemautismoanda') }} AS triagemautismoanda,
            {{ process_null('triagemautismoolhanosolhos') }} AS triagemautismoolhanosolhos,
            {{ process_null('triagemautismoimita') }} AS triagemautismoimita,
            {{ process_null('triagemautismoolhando') }} AS triagemautismoolhando,
            {{ process_null('triagemautismofazolhar') }} AS triagemautismofazolhar,
            {{ process_null('triagemautismocompreendepedido') }} AS triagemautismocompreendepedido,
            {{ process_null('triagemautismoolhacomosente') }} AS triagemautismoolhacomosente,
            {{ process_null('triagemautismogostaatividades') }} AS triagemautismogostaatividades,
            SAFE_CAST({{ process_null('triagemautismoscore') }} AS NUMERIC) AS triagemautismoscore,
            {{ process_null('triagemautismoclassificacao') }} AS triagemautismoclassificacao,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM saudecrianca_deduplicados
    )

SELECT
    *
FROM fato_saudecrianca