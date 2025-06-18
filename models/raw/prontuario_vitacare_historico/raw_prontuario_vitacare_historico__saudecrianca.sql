{{
    config(
        alias="saudecrianca", 
        materialized="table",
        schema="brutos_prontuario_vitacare_historico",
    )
}}

WITH

    source_saudecrianca AS (
        SELECT 
            CONCAT(
                NULLIF({{ remove_double_quotes('id_cnes') }}, ''), 
                '.',
                NULLIF({{ remove_double_quotes('acto_id') }}, '')
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
            SAFE_CAST({{ remove_double_quotes('acto_id') }} AS NUMERIC) AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ process_null(remove_double_quotes('ppneonatal')) }} AS ppneonatal,
            {{ process_null(remove_double_quotes('ppneonatalobs')) }} AS ppneonatalobs,
            {{ process_null(remove_double_quotes('anomaliascongenitas')) }} AS anomaliascongenitas,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('regcrescaltura') }} AS NUMERIC)) }} AS regcrescaltura,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('regcrescpeso') }} AS NUMERIC)) }} AS regcrescpeso,
            {{ process_null(remove_double_quotes('observacoes')) }} AS observacoes,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('regcrescimc') }} AS NUMERIC)) }} AS regcrescimc,
            {{ process_null(remove_double_quotes('rastreios')) }} AS rastreios,
            {{ process_null(remove_double_quotes('anamaliascongobs')) }} AS anamaliascongobs,
            {{ process_null(remove_double_quotes('registocrescimentioobs')) }} AS registocrescimentioobs,
            {{ process_null(remove_double_quotes('tipoparto')) }} AS tipoparto,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('pesonascer') }} AS NUMERIC)) }} AS pesonascer,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('comprimento') }} AS NUMERIC)) }} AS comprimento,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('perimetrocefalico') }} AS NUMERIC)) }} AS perimetrocefalico,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('indiceapgar1minuto') }} AS NUMERIC)) }} AS indiceapgar1minuto,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('indiceapgar5minuto') }} AS NUMERIC)) }} AS indiceapgar5minuto,
            {{ process_null(remove_double_quotes('reanimacao')) }} AS reanimacao,
            {{ process_null(remove_double_quotes('exameocularqueixas')) }} AS exameocularqueixas,
            {{ process_null(remove_double_quotes('exameoculardoencas')) }} AS exameoculardoencas,
            {{ process_null(remove_double_quotes('exameocularnecessitaoculos')) }} AS exameocularnecessitaoculos,
            {{ process_null(remove_double_quotes('exameocularreferencia')) }} AS exameocularreferencia,
            {{ process_null(remove_double_quotes('exameocularestado')) }} AS exameocularestado,
            {{ process_null(remove_double_quotes('exameocularobservacoes')) }} AS exameocularobservacoes,
            {{ process_null(remove_double_quotes('testepezinho')) }} AS testepezinho,
            {{ process_null(remove_double_quotes('testepezinhodoencas')) }} AS testepezinhodoencas,
            {{ process_null(remove_double_quotes('reflexovermelho')) }} AS reflexovermelho,
            {{ process_null(remove_double_quotes('testeorelhinha')) }} AS testeorelhinha,
            {{ process_null(remove_double_quotes('periodoprenatal')) }} AS periodoprenatal,
            {{ process_null(remove_double_quotes('denverperiodotemporal')) }} AS denverperiodotemporal,
            {{ process_null(remove_double_quotes('denverpostura')) }} AS denverpostura,
            {{ process_null(remove_double_quotes('denverobservarosto')) }} AS denverobservarosto,
            {{ process_null(remove_double_quotes('denverreagesom')) }} AS denverreagesom,
            {{ process_null(remove_double_quotes('denverelevacabeca')) }} AS denverelevacabeca,
            {{ process_null(remove_double_quotes('denversorriso')) }} AS denversorriso,
            {{ process_null(remove_double_quotes('denverabremaos')) }} AS denverabremaos,
            {{ process_null(remove_double_quotes('denveremitesons')) }} AS denveremitesons,
            {{ process_null(remove_double_quotes('denvermovimentos')) }} AS denvermovimentos,
            {{ process_null(remove_double_quotes('denvercontatosocial')) }} AS denvercontatosocial,
            {{ process_null(remove_double_quotes('denverseguraobjetos')) }} AS denverseguraobjetos,
            {{ process_null(remove_double_quotes('denveremitesons24meses')) }} AS denveremitesons24meses,
            {{ process_null(remove_double_quotes('denverbrucolevantacabeca')) }} AS denverbrucolevantacabeca,
            {{ process_null(remove_double_quotes('denverbuscaobjectos')) }} AS denverbuscaobjectos,
            {{ process_null(remove_double_quotes('denverlevaobjectos')) }} AS denverlevaobjectos,
            {{ process_null(remove_double_quotes('denverlocalizasom')) }} AS denverlocalizasom,
            {{ process_null(remove_double_quotes('denvermudaposicaorola')) }} AS denvermudaposicaorola,
            {{ process_null(remove_double_quotes('denverbrincaescondeachou')) }} AS denverbrincaescondeachou,
            {{ process_null(remove_double_quotes('denvertransfereobjectos')) }} AS denvertransfereobjectos,
            {{ process_null(remove_double_quotes('denverduplicasilabas')) }} AS denverduplicasilabas,
            {{ process_null(remove_double_quotes('denversentasemapoio')) }} AS denversentasemapoio,
            {{ process_null(remove_double_quotes('denverimitagestos')) }} AS denverimitagestos,
            {{ process_null(remove_double_quotes('denverfazpinca')) }} AS denverfazpinca,
            {{ process_null(remove_double_quotes('denverproduzjargao')) }} AS denverproduzjargao,
            {{ process_null(remove_double_quotes('denverandacomapoio')) }} AS denverandacomapoio,
            {{ process_null(remove_double_quotes('denvermostraroquequer')) }} AS denvermostraroquequer,
            {{ process_null(remove_double_quotes('denvercolocablocoscaneca')) }} AS denvercolocablocoscaneca,
            {{ process_null(remove_double_quotes('denverdizumapalavra')) }} AS denverdizumapalavra,
            {{ process_null(remove_double_quotes('denverandaparatras')) }} AS denverandaparatras,
            {{ process_null(remove_double_quotes('denvertiraroupa')) }} AS denvertiraroupa,
            {{ process_null(remove_double_quotes('denverconstroitorre3cubos')) }} AS denverconstroitorre3cubos,
            {{ process_null(remove_double_quotes('denveraponta2figuras')) }} AS denveraponta2figuras,
            {{ process_null(remove_double_quotes('denverchutabola')) }} AS denverchutabola,
            {{ process_null(remove_double_quotes('denvervestecomsupervisao')) }} AS denvervestecomsupervisao,
            {{ process_null(remove_double_quotes('denverconstroitorre6cubos')) }} AS denverconstroitorre6cubos,
            {{ process_null(remove_double_quotes('denverfrases2palavras')) }} AS denverfrases2palavras,
            {{ process_null(remove_double_quotes('denverpulaambospes')) }} AS denverpulaambospes,
            {{ process_null(remove_double_quotes('denverbrincacomoutrascriancas')) }} AS denverbrincacomoutrascriancas,
            {{ process_null(remove_double_quotes('denverimitalinhavertical')) }} AS denverimitalinhavertical,
            {{ process_null(remove_double_quotes('denverreconhece2acoes')) }} AS denverreconhece2acoes,
            {{ process_null(remove_double_quotes('denverarremessabola')) }} AS denverarremessabola,
            {{ process_null(remove_double_quotes('rastreiostestepezinhoobservacoes')) }} AS rastreiostestepezinhoobservacoes,
            {{ process_null(remove_double_quotes('rastreiosreflexovermelhoobservacoes')) }} AS rastreiosreflexovermelhoobservacoes,
            {{ process_null(remove_double_quotes('rastreiostesteorelhinhaobservacoes')) }} AS rastreiostesteorelhinhaobservacoes,
            {{ process_null(remove_double_quotes('tecnicasespeciais')) }} AS tecnicasespeciais,
            {{ process_null(remove_double_quotes('tecnicasespeciaisoutras')) }} AS tecnicasespeciaisoutras,
            {{ process_null(remove_double_quotes('sintomasacuidadevisual')) }} AS sintomasacuidadevisual,
            {{ process_null(remove_double_quotes('snellendistanciaesq')) }} AS snellendistanciaesq,
            {{ process_null(remove_double_quotes('snellendistanciadir')) }} AS snellendistanciadir,
            {{ process_null(remove_double_quotes('snellentipooptotiposesq')) }} AS snellentipooptotiposesq,
            {{ process_null(remove_double_quotes('snellentipooptotiposdir')) }} AS snellentipooptotiposdir,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('comcorrecaoesq') }} AS NUMERIC)) }} AS comcorrecaoesq,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('semcorrecaoesq') }} AS NUMERIC)) }} AS semcorrecaoesq,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('comcorrecaodir') }} AS NUMERIC)) }} AS comcorrecaodir,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('semcorrecaodir') }} AS NUMERIC)) }} AS semcorrecaodir,
            {{ process_null(remove_double_quotes('acuidadevisualconduta')) }} AS acuidadevisualconduta,
            {{ process_null(remove_double_quotes('triagemautismoolhaobjeto')) }} AS triagemautismoolhaobjeto,
            {{ process_null(remove_double_quotes('triagemautismopodesersurda')) }} AS triagemautismopodesersurda,
            {{ process_null(remove_double_quotes('triagemautismobrincafazdecontas')) }} AS triagemautismobrincafazdecontas,
            {{ process_null(remove_double_quotes('triagemautismosubirnascoisas')) }} AS triagemautismosubirnascoisas,
            {{ process_null(remove_double_quotes('triagemautismomovimentosestranhos')) }} AS triagemautismomovimentosestranhos,
            {{ process_null(remove_double_quotes('triagemautismoapontaodedopedir')) }} AS triagemautismoapontaodedopedir,
            {{ process_null(remove_double_quotes('triagemautismoapontaodedomostrar')) }} AS triagemautismoapontaodedomostrar,
            {{ process_null(remove_double_quotes('triagemautismointeresseoutrascriancas')) }} AS triagemautismointeresseoutrascriancas,
            {{ process_null(remove_double_quotes('triagemautismotrazcoisas')) }} AS triagemautismotrazcoisas,
            {{ process_null(remove_double_quotes('triagemautismorespondepelonome')) }} AS triagemautismorespondepelonome,
            {{ process_null(remove_double_quotes('triagemautismosorri')) }} AS triagemautismosorri,
            {{ process_null(remove_double_quotes('triagemautismoincomodadabarulho')) }} AS triagemautismoincomodadabarulho,
            {{ process_null(remove_double_quotes('triagemautismoanda')) }} AS triagemautismoanda,
            {{ process_null(remove_double_quotes('triagemautismoolhanosolhos')) }} AS triagemautismoolhanosolhos,
            {{ process_null(remove_double_quotes('triagemautismoimita')) }} AS triagemautismoimita,
            {{ process_null(remove_double_quotes('triagemautismoolhando')) }} AS triagemautismoolhando,
            {{ process_null(remove_double_quotes('triagemautismofazolhar')) }} AS triagemautismofazolhar,
            {{ process_null(remove_double_quotes('triagemautismocompreendepedido')) }} AS triagemautismocompreendepedido,
            {{ process_null(remove_double_quotes('triagemautismoolhacomosente')) }} AS triagemautismoolhacomosente,
            {{ process_null(remove_double_quotes('triagemautismogostaatividades')) }} AS triagemautismogostaatividades,
            {{ process_null(SAFE_CAST({{ remove_double_quotes('triagemautismoscore') }} AS NUMERIC)) }} AS triagemautismoscore,
            {{ process_null(remove_double_quotes('triagemautismoclassificacao')) }} AS triagemautismoclassificacao,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM saudecrianca_deduplicados
    )

SELECT
    *
FROM fato_saudecrianca