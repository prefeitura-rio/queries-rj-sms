{{
    config(
        alias="saude_crianca", 
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key=['id_prontuario_global'],
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

            {{ process_null('ppneonatal') }} AS pp_neonatal,
            {{ process_null('ppneonatalobs') }} AS pp_neonatal_obs,
            {{ process_null('anomaliascongenitas') }} AS anomalias_congenitas,
            {{ process_null('regcrescaltura') }} AS reg_cresc_altura,
            {{ process_null('regcrescpeso') }} AS reg_cresc_peso,
            {{ process_null('observacoes') }} AS observacoes,
            {{ process_null('regcrescimc') }} AS reg_cresc_imc,
            {{ process_null('rastreios') }} AS rastreios,
            {{ process_null('anamaliascongobs') }} AS anamalias_cong_obs,
            {{ process_null('registocrescimentioobs') }} AS registocrescimentio_obs,
            {{ process_null('tipoparto') }} AS tipo_parto,
            {{ process_null('pesonascer') }} AS peso_nascer,
            {{ process_null('comprimento') }} AS comprimento,
            {{ process_null('perimetrocefalico') }} AS perimetro_cefalico,
            {{ process_null('indiceapgar1minuto') }} AS indice_apgar_1_minuto,
            {{ process_null('indiceapgar5minuto') }} AS indice_apgar_5_minuto,
            {{ process_null('reanimacao') }} AS reanimacao,
            {{ process_null('exameocularqueixas') }} AS exame_ocular_queixas,
            {{ process_null('exameoculardoencas') }} AS exame_ocular_doencas,
            {{ process_null('exameocularnecessitaoculos') }} AS exame_ocular_necessita_oculos,
            {{ process_null('exameocularreferencia') }} AS exame_ocular_referencia,
            {{ process_null('exameocularestado') }} AS exame_ocular_estado,
            {{ process_null('exameocularobservacoes') }} AS exame_ocular_observacoes,
            {{ process_null('testepezinho') }} AS teste_pezinho,
            {{ process_null('testepezinhodoencas') }} AS teste_pezinho_doencas,
            {{ process_null('reflexovermelho') }} AS reflexo_vermelho,
            {{ process_null('testeorelhinha') }} AS teste_orelhinha,
            {{ process_null('periodoprenatal') }} AS periodo_prenatal,
            {{ process_null('denverperiodotemporal') }} AS denver_periodo_temporal,
            {{ process_null('denverpostura') }} AS denver_postura,
            {{ process_null('denverobservarosto') }} AS denver_observa_rosto,
            {{ process_null('denverreagesom') }} AS denver_reage_som,
            {{ process_null('denverelevacabeca') }} AS denver_eleva_cabeca,
            {{ process_null('denversorriso') }} AS denver_sorriso,
            {{ process_null('denverabremaos') }} AS denver_abre_maos,
            {{ process_null('denveremitesons') }} AS denver_emite_sons,
            {{ process_null('denvermovimentos') }} AS denver_movimentos,
            {{ process_null('denvercontatosocial') }} AS denver_contato_social,
            {{ process_null('denverseguraobjetos') }} AS denver_segura_objetos,
            {{ process_null('denveremitesons24meses') }} AS denver_emite_sons_24_meses,
            {{ process_null('denverbrucolevantacabeca') }} AS denver_bruco_levanta_cabeca,
            {{ process_null('denverbuscaobjectos') }} AS denver_busca_objetos,
            {{ process_null('denverlevaobjectos') }} AS denver_leva_objetos,
            {{ process_null('denverlocalizasom') }} AS denver_localiza_som,
            {{ process_null('denvermudaposicaorola') }} AS denver_muda_posicao_rola,
            {{ process_null('denverbrincaescondeachou') }} AS denver_brinca_esconde_achou,
            {{ process_null('denvertransfereobjectos') }} AS denver_transfere_objetos,
            {{ process_null('denverduplicasilabas') }} AS denver_duplica_silabas,
            {{ process_null('denversentasemapoio') }} AS denver_senta_sem_apoio,
            {{ process_null('denverimitagestos') }} AS denver_imita_gestos,
            {{ process_null('denverfazpinca') }} AS denver_faz_pinca,
            {{ process_null('denverproduzjargao') }} AS denver_produz_jargao,
            {{ process_null('denverandacomapoio') }} AS denver_anda_com_apoio,
            {{ process_null('denvermostraroquequer') }} AS denver_mostra_o_que_quer,
            {{ process_null('denvercolocablocoscaneca') }} AS denver_coloca_blocos_caneca,
            {{ process_null('denverdizumapalavra') }} AS denver_diz_uma_palavra,
            {{ process_null('denverandaparatras') }} AS denver_anda_para_tras,
            {{ process_null('denvertiraroupa') }} AS denver_tira_roupa,
            {{ process_null('denverconstroitorre3cubos') }} AS denver_constroi_torre_3_cubos,
            {{ process_null('denveraponta2figuras') }} AS denver_aponta_2_figuras,
            {{ process_null('denverchutabola') }} AS denver_chuta_bola,
            {{ process_null('denvervestecomsupervisao') }} AS denver_veste_com_supervisao,
            {{ process_null('denverconstroitorre6cubos') }} AS denver_constroi_torre_6_cubos,
            {{ process_null('denverfrases2palavras') }} AS denver_frases_2_palavras,
            {{ process_null('denverpulaambospes') }} AS denver_pula_ambos_pes,
            {{ process_null('denverbrincacomoutrascriancas') }} AS denver_brinca_com_outras_criancas,
            {{ process_null('denverimitalinhavertical') }} AS denver_imita_linha_vertical,
            {{ process_null('denverreconhece2acoes') }} AS denver_reconhece_2_acoes,
            {{ process_null('denverarremessabola') }} AS denver_arremessa_bola,
            {{ process_null('rastreiostestepezinhoobservacoes') }} AS rastreios_testepezinho_observacoes,
            {{ process_null('rastreiosreflexovermelhoobservacoes') }} AS rastreios_reflexovermelho_observacoes,
            {{ process_null('rastreiostesteorelhinhaobservacoes') }} AS rastreios_testeorelhinha_observacoes,
            {{ process_null('tecnicasespeciais') }} AS tecnicas_especiais,
            {{ process_null('tecnicasespeciaisoutras') }} AS tecnicas_especiais_outras,
            {{ process_null('sintomasacuidadevisual') }} AS sintomas_acuidade_visual,
            {{ process_null('snellendistanciaesq') }} AS snellen_distancia_esq,
            {{ process_null('snellendistanciadir') }} AS snellen_distancia_dir,
            {{ process_null('snellentipooptotiposesq') }} AS snellen_tipo_optotipos_esq,
            {{ process_null('snellentipooptotiposdir') }} AS snellen_tipo_optotipos_dir,
            {{ process_null('comcorrecaoesq') }} AS com_correcao_esq,
            {{ process_null('semcorrecaoesq') }} AS sem_correcao_esq,
            {{ process_null('comcorrecaodir') }} AS com_correcao_dir,
            {{ process_null('semcorrecaodir') }} AS sem_correcao_dir,
            {{ process_null('acuidadevisualconduta') }} AS acuidade_visual_conduta,
            {{ process_null('triagemautismoolhaobjeto') }} AS triagem_autismo_olha_objeto,
            {{ process_null('triagemautismopodesersurda') }} AS triagem_autismo_pode_ser_surda,
            {{ process_null('triagemautismobrincafazdecontas') }} AS triagem_autismo_brinca_faz_de_contas,
            {{ process_null('triagemautismosubirnascoisas') }} AS triagem_autismo_subir_nas_coisas,
            {{ process_null('triagemautismomovimentosestranhos') }} AS triagem_autismo_movimentos_estranhos,
            {{ process_null('triagemautismoapontaodedopedir') }} AS triagem_autismo_aponta_dedo_pedir,
            {{ process_null('triagemautismoapontaodedomostrar') }} AS triagem_autismo_aponta_dedo_mostrar,
            {{ process_null('triagemautismointeresseoutrascriancas') }} AS triagem_autismo_interesse_outras_criancas,
            {{ process_null('triagemautismotrazcoisas') }} AS triagem_autismo_traz_coisas,
            {{ process_null('triagemautismorespondepelonome') }} AS triagem_autismo_responde_pelo_nome,
            {{ process_null('triagemautismosorri') }} AS triagem_autismo_sorri,
            {{ process_null('triagemautismoincomodadabarulho') }} AS triagem_autismo_incomodada_barulho,
            {{ process_null('triagemautismoanda') }} AS triagem_autismo_anda,
            {{ process_null('triagemautismoolhanosolhos') }} AS triagem_autismo_olha_nos_olhos,
            {{ process_null('triagemautismoimita') }} AS triagem_autismo_imita,
            {{ process_null('triagemautismoolhando') }} AS triagem_autismo_olhando,
            {{ process_null('triagemautismofazolhar') }} AS triagem_autismo_faz_olhar,
            {{ process_null('triagemautismocompreendepedido') }} AS triagem_autismo_compreende_pedido,
            {{ process_null('triagemautismoolhacomosente') }} AS triagem_autismo_olha_como_sente,
            {{ process_null('triagemautismogostaatividades') }} AS triagem_autismo_gosta_atividades,
            {{ process_null('triagemautismoscore') }} AS triagem_autismo_score,
            {{ process_null('triagemautismoclassificacao') }} AS triagem_autismo_classificacao,
   
            extracted_at AS loaded_at,
            DATE(SAFE_CAST(extracted_at AS DATETIME)) AS data_particao
        FROM saudecrianca_deduplicados
    ),

    -- Filtro temporário para remover registros anteriores à carga oficial (24/06/2025 17:15)
    fato_filtrado AS (
        SELECT *
        FROM fato_saudecrianca
        WHERE PARSE_TIMESTAMP('%F %H:%M:%E6S', loaded_at) > TIMESTAMP('2025-06-24 17:15:00.000000')
    )

SELECT
    *
FROM fato_filtrado