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
        FROM {{ source('brutos_vitacare_historic_staging', 'SAUDECRIANCA') }} 
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
            {{ remove_double_quotes('acto_id') }} AS id_prontuario_local,
            {{ remove_double_quotes('id_cnes') }} AS cnes_unidade,

            {{ remove_double_quotes('ppneonatal') }} AS ppneonatal,
            {{ remove_double_quotes('ppneonatalobs') }} AS ppneonatalobs,
            {{ remove_double_quotes('anomaliascongenitas') }} AS anomaliascongenitas,
            {{ remove_double_quotes('regcrescaltura') }} AS regcrescaltura,
            {{ remove_double_quotes('regcrescpeso') }} AS regcrescpeso,
            {{ remove_double_quotes('observacoes') }} AS observacoes,
            {{ remove_double_quotes('regcrescimc') }} AS regcrescimc,
            {{ remove_double_quotes('rastreios') }} AS rastreios,
            {{ remove_double_quotes('anamaliascongobs') }} AS anamaliascongobs,
            {{ remove_double_quotes('registocrescimentioobs') }} AS registocrescimentioobs,
            {{ remove_double_quotes('tipoparto') }} AS tipoparto,
            {{ remove_double_quotes('pesonascer') }} AS pesonascer,
            {{ remove_double_quotes('comprimento') }} AS comprimento,
            {{ remove_double_quotes('perimetrocefalico') }} AS perimetrocefalico,
            {{ remove_double_quotes('indiceapgar1minuto') }} AS indiceapgar1minuto,
            {{ remove_double_quotes('indiceapgar5minuto') }} AS indiceapgar5minuto,
            {{ remove_double_quotes('reanimacao') }} AS reanimacao,
            {{ remove_double_quotes('exameocularqueixas') }} AS exameocularqueixas,
            {{ remove_double_quotes('exameoculardoencas') }} AS exameoculardoencas,
            {{ remove_double_quotes('exameocularnecessitaoculos') }} AS exameocularnecessitaoculos,
            {{ remove_double_quotes('exameocularreferencia') }} AS exameocularreferencia,
            {{ remove_double_quotes('exameocularestado') }} AS exameocularestado,
            {{ remove_double_quotes('exameocularobservacoes') }} AS exameocularobservacoes,
            {{ remove_double_quotes('testepezinho') }} AS testepezinho,
            {{ remove_double_quotes('testepezinhodoencas') }} AS testepezinhodoencas,
            {{ remove_double_quotes('reflexovermelho') }} AS reflexovermelho,
            {{ remove_double_quotes('testeorelhinha') }} AS testeorelhinha,
            {{ remove_double_quotes('periodoprenatal') }} AS periodoprenatal,
            {{ remove_double_quotes('denverperiodotemporal') }} AS denverperiodotemporal,
            {{ remove_double_quotes('denverpostura') }} AS denverpostura,
            {{ remove_double_quotes('denverobservarosto') }} AS denverobservarosto,
            {{ remove_double_quotes('denverreagesom') }} AS denverreagesom,
            {{ remove_double_quotes('denverelevacabeca') }} AS denverelevacabeca,
            {{ remove_double_quotes('denversorriso') }} AS denversorriso,
            {{ remove_double_quotes('denverabremaos') }} AS denverabremaos,
            {{ remove_double_quotes('denveremitesons') }} AS denveremitesons,
            {{ remove_double_quotes('denvermovimentos') }} AS denvermovimentos,
            {{ remove_double_quotes('denvercontatosocial') }} AS denvercontatosocial,
            {{ remove_double_quotes('denverseguraobjetos') }} AS denverseguraobjetos,
            {{ remove_double_quotes('denveremitesons24meses') }} AS denveremitesons24meses,
            {{ remove_double_quotes('denverbrucolevantacabeca') }} AS denverbrucolevantacabeca,
            {{ remove_double_quotes('denverbuscaobjectos') }} AS denverbuscaobjectos,
            {{ remove_double_quotes('denverlevaobjectos') }} AS denverlevaobjectos,
            {{ remove_double_quotes('denverlocalizasom') }} AS denverlocalizasom,
            {{ remove_double_quotes('denvermudaposicaorola') }} AS denvermudaposicaorola,
            {{ remove_double_quotes('denverbrincaescondeachou') }} AS denverbrincaescondeachou,
            {{ remove_double_quotes('denvertransfereobjectos') }} AS denvertransfereobjectos,
            {{ remove_double_quotes('denverduplicasilabas') }} AS denverduplicasilabas,
            {{ remove_double_quotes('denversentasemapoio') }} AS denversentasemapoio,
            {{ remove_double_quotes('denverimitagestos') }} AS denverimitagestos,
            {{ remove_double_quotes('denverfazpinca') }} AS denverfazpinca,
            {{ remove_double_quotes('denverproduzjargao') }} AS denverproduzjargao,
            {{ remove_double_quotes('denverandacomapoio') }} AS denverandacomapoio,
            {{ remove_double_quotes('denvermostraroquequer') }} AS denvermostraroquequer,
            {{ remove_double_quotes('denvercolocablocoscaneca') }} AS denvercolocablocoscaneca,
            {{ remove_double_quotes('denverdizumapalavra') }} AS denverdizumapalavra,
            {{ remove_double_quotes('denverandasemapoio') }} AS denverandasemapoio,
            {{ remove_double_quotes('denverusacolhergarfo') }} AS denverusacolhergarfo,
            {{ remove_double_quotes('denverconstroitorre2cubos') }} AS denverconstroitorre2cubos,
            {{ remove_double_quotes('denverfala3palavras') }} AS denverfala3palavras,
            {{ remove_double_quotes('denverandaparatras') }} AS denverandaparatras,
            {{ remove_double_quotes('denvertiraroupa') }} AS denvertiraroupa,
            {{ remove_double_quotes('denverconstroitorre3cubos') }} AS denverconstroitorre3cubos,
            {{ remove_double_quotes('denveraponta2figuras') }} AS denveraponta2figuras,
            {{ remove_double_quotes('denverchutabola') }} AS denverchutabola,
            {{ remove_double_quotes('denvervestecomsupervisao') }} AS denvervestecomsupervisao,
            {{ remove_double_quotes('denverconstroitorre6cubos') }} AS denverconstroitorre6cubos,
            {{ remove_double_quotes('denverfrases2palavras') }} AS denverfrases2palavras,
            {{ remove_double_quotes('denverpulaambospes') }} AS denverpulaambospes,
            {{ remove_double_quotes('denverbrincacomoutrascriancas') }} AS denverbrincacomoutrascriancas,
            {{ remove_double_quotes('denverimitalinhavertical') }} AS denverimitalinhavertical,
            {{ remove_double_quotes('denverreconhece2acoes') }} AS denverreconhece2acoes,
            {{ remove_double_quotes('denverarremessabola') }} AS denverarremessabola,
            {{ remove_double_quotes('rastreiostestepezinhoobservacoes') }} AS rastreiostestepezinhoobservacoes,
            {{ remove_double_quotes('rastreiosreflexovermelhoobservacoes') }} AS rastreiosreflexovermelhoobservacoes,
            {{ remove_double_quotes('rastreiostesteorelhinhaobservacoes') }} AS rastreiostesteorelhinhaobservacoes,
            {{ remove_double_quotes('tecnicasespeciais') }} AS tecnicasespeciais,
            {{ remove_double_quotes('tecnicasespeciaisoutras') }} AS tecnicasespeciaisoutras,
            {{ remove_double_quotes('sintomasacuidadevisual') }} AS sintomasacuidadevisual,
            {{ remove_double_quotes('snellendistanciaesq') }} AS snellendistanciaesq,
            {{ remove_double_quotes('snellendistanciadir') }} AS snellendistanciadir,
            {{ remove_double_quotes('snellentipooptotiposesq') }} AS snellentipooptotiposesq,
            {{ remove_double_quotes('snellentipooptotiposdir') }} AS snellentipooptotiposdir,
            {{ remove_double_quotes('comcorrecaoesq') }} AS comcorrecaoesq,
            {{ remove_double_quotes('semcorrecaoesq') }} AS semcorrecaoesq,
            {{ remove_double_quotes('comcorrecaodir') }} AS comcorrecaodir,
            {{ remove_double_quotes('semcorrecaodir') }} AS semcorrecaodir,
            {{ remove_double_quotes('acuidadevisualconduta') }} AS acuidadevisualconduta,
            {{ remove_double_quotes('triagemautismoolhaobjeto') }} AS triagemautismoolhaobjeto,
            {{ remove_double_quotes('triagemautismopodesersurda') }} AS triagemautismopodesersurda,
            {{ remove_double_quotes('triagemautismobrincafazdecontas') }} AS triagemautismobrincafazdecontas,
            {{ remove_double_quotes('triagemautismosubirnascoisas') }} AS triagemautismosubirnascoisas,
            {{ remove_double_quotes('triagemautismomovimentosestranhos') }} AS triagemautismomovimentosestranhos,
            {{ remove_double_quotes('triagemautismoapontaodedopedir') }} AS triagemautismoapontaodedopedir,
            {{ remove_double_quotes('triagemautismoapontaodedomostrar') }} AS triagemautismoapontaodedomostrar,
            {{ remove_double_quotes('triagemautismointeresseoutrascriancas') }} AS triagemautismointeresseoutrascriancas,
            {{ remove_double_quotes('triagemautismotrazcoisas') }} AS triagemautismotrazcoisas,
            {{ remove_double_quotes('triagemautismorespondepelonome') }} AS triagemautismorespondepelonome,
            {{ remove_double_quotes('triagemautismosorri') }} AS triagemautismosorri,
            {{ remove_double_quotes('triagemautismoincomodadabarulho') }} AS triagemautismoincomodadabarulho,
            {{ remove_double_quotes('triagemautismoanda') }} AS triagemautismoanda,
            {{ remove_double_quotes('triagemautismoolhanosolhos') }} AS triagemautismoolhanosolhos,
            {{ remove_double_quotes('triagemautismoimita') }} AS triagemautismoimita,
            {{ remove_double_quotes('triagemautismoolhando') }} AS triagemautismoolhando,
            {{ remove_double_quotes('triagemautismofazolhar') }} AS triagemautismofazolhar,
            {{ remove_double_quotes('triagemautismocompreendepedido') }} AS triagemautismocompreendepedido,
            {{ remove_double_quotes('triagemautismoolhacomosente') }} AS triagemautismoolhacomosente,
            {{ remove_double_quotes('triagemautismogostaatividades') }} AS triagemautismogostaatividades,
            {{ remove_double_quotes('triagemautismoscore') }} AS triagemautismoscore,
            {{ remove_double_quotes('triagemautismoclassificacao') }} AS triagemautismoclassificacao,
   
            {{ remove_double_quotes('extracted_at') }} AS extracted_at
        FROM saudecrianca_deduplicados
    )

SELECT
    *
FROM fato_saudecrianca