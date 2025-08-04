{{ config(
    alias="saude_crianca",
    materialized="incremental",
    schema="brutos_prontuario_vitacare_api",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"}
) }}

WITH bruto_atendimento AS (
  SELECT
    CAST(source_id AS STRING) AS id_prontuario_local,
    CONCAT(NULLIF(CAST(payload_cnes AS STRING), ''), '.', NULLIF(CAST(source_id AS STRING), '')) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    data,
    DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME)) AS data_particao
  FROM {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
  {% if is_incremental() %}
    WHERE DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME)) > (SELECT MAX(data_particao) FROM {{ this }})
  {% endif %}
),

saude_crianca_extraida AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    (
    SELECT
        JSON_EXTRACT_SCALAR(elem, '$')
    FROM UNNEST(COALESCE(JSON_EXTRACT_ARRAY(data, '$.saude_crianca[0].ppneonatal'), [])) AS elem
    LIMIT 1
    ) AS pp_neonatal,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].ppneonatalobs') AS pp_neonatal_obs,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].anomaliascongenitas') AS anomalias_congenitas,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].regcrescaltura') AS FLOAT64) AS reg_cresc_altura,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].regcrescPeso') AS FLOAT64) AS reg_cresc_peso,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].observacoes') AS observacoes,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].regcrescimc') AS FLOAT64) AS reg_cresc_imc,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].rastreios') AS rastreios,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].anamaliascongobs') AS anamalias_cong_obs,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].registocrescimentioobs') AS registocrescimentio_obs,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].tipoparto') AS tipo_parto,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].pesonascer') AS FLOAT64) AS peso_nascer,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].comprimento') AS FLOAT64) AS comprimento,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].perimetrocefálico') AS FLOAT64) AS perimetro_cefalico,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].indiceapgar1minuto') AS FLOAT64) AS indice_apgar_1_minuto,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].indiceapgar5minuto') AS FLOAT64) AS indice_apgar_5_minuto,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].reanimacao') AS reanimacao,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].exameocularqueixas') AS exame_ocular_queixas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].exameoculardoencas') AS exame_ocular_doencas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].exameocularnecessitaoculos') AS exame_ocular_necessita_oculos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].exameocularreferencia') AS exame_ocular_referencia,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].exameocularestado') AS exame_ocular_estado,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].exameocularobservacoes') AS exame_ocular_observacoes,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].testepezinho') AS teste_pezinho,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].testepezinhodoencas') AS teste_pezinho_doencas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].reflexovermelho') AS reflexo_vermelho,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].testeorelhinha') AS teste_orelhinha,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].periodoprenatal') AS periodo_prenatal,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverperiodotemporal') AS denver_periodo_temporal,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverpostura') AS denver_postura,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverobservarosto') AS denver_observa_rosto,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverreagesom') AS denver_reage_som,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverelevacabeca') AS denver_eleva_cabeca,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denversorriso') AS denver_sorriso,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverabremaos') AS denver_abre_maos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denveremitesons') AS denver_emite_sons,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denvermovimentos') AS denver_movimentos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denvercontatosocial') AS denver_contato_social,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverseguraobjetos') AS denver_segura_objetos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denveremitesons24meses') AS denver_emite_sons_24_meses,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverbrucolevantacabeca') AS denver_bruco_levanta_cabeca,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverbuscaobjectos') AS denver_busca_objetos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverlevaobjectos') AS denver_leva_objetos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverlocalizasom') AS denver_localiza_som,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denvermudaposicaorola') AS denver_muda_posicao_rola,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverbrincaescondeachou') AS denver_brinca_esconde_achou,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denvertransfereobjectos') AS denver_transfere_objetos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverduplicasilabas') AS denver_duplica_silabas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denversentasemapoio') AS denver_senta_sem_apoio,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverimitagestos') AS denver_imita_gestos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverfazpinca') AS denver_faz_pinca,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverproduzjargao') AS denver_produz_jargao,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverandacomapoio') AS denver_anda_com_apoio,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denvermostraoquequer') AS denver_mostra_o_que_quer,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denvercolocablocoscaneca') AS denver_coloca_blocos_caneca,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverdizumapalavra') AS denver_diz_uma_palavra,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverandaparatras') AS denver_anda_para_tras,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denvertiraroupa') AS denver_tira_roupa,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverconstroitorre3cubos') AS denver_constroi_torre_3_cubos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denveraponta2figuras') AS denver_aponta_2_figuras,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverchutabola') AS denver_chuta_bola,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverbrincacomoutrascriancas') AS denver_brinca_com_outras_criancas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverimitalinhavertical') AS denver_imita_linha_vertical,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverreconhece2acoes') AS denver_reconhece_2_acoes,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverarremessabola') AS denver_arremessa_bola,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].rastreiostestepezinhoobservacoes') AS rastreios_testepezinho_observacoes,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].rastreiosreflexovermelhoobservacoes') AS rastreios_reflexovermelho_observacoes,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].rastreiostesteorelhinhaobservacoes') AS rastreios_testeorelhinha_observacoes,
    
    (
    SELECT JSON_EXTRACT_SCALAR(elem, '$')
    FROM UNNEST(COALESCE(JSON_EXTRACT_ARRAY(data, '$.saude_crianca[0].tecnicasespeciais'), [])) AS elem
    LIMIT 1
    ) AS tecnicas_especiais,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].tecnicasespeciaisoutras') AS tecnicas_especiais_outras,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].sintomasacuidadevisual') AS sintomas_acuidade_visual,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].snellendistanciaesq') AS snellen_distancia_esq,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].snellendistanciadir') AS snellen_distancia_dir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].snellentipooptotiposesq') AS snellen_tipo_optotipos_esq,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].snellentipooptotiposdir') AS snellen_tipo_optotipos_dir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].comcorrecaoesq') AS com_correcao_esq,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].semcorrecaoesq') AS sem_correcao_esq,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].comcorrecaodir') AS com_correcao_dir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].semcorrecaodir') AS sem_correcao_dir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].acuidadevisualconduta') AS acuidade_visual_conduta,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoolhaobjeto') AS triagem_autismo_olha_objeto,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismopodesersurda') AS triagem_autismo_pode_ser_surda,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismobrincafazdecontas') AS triagem_autismo_brinca_faz_de_contas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismosubirnascoisas') AS triagem_autismo_subir_nas_coisas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismomovimentosestranhos') AS triagem_autismo_movimentos_estranhos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoapontaodedopedir') AS triagem_autismo_aponta_dedo_pedir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoapontaodedomostrar') AS triagem_autismo_aponta_dedo_mostrar,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismointeresseoutrascriancas') AS triagem_autismo_interesse_outras_criancas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismotrazcoisas') AS triagem_autismo_traz_coisas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismorespondepelonome') AS triagem_autismo_responde_pelo_nome,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoscore') AS FLOAT64) AS triagem_autismo_score,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismosurri') AS triagem_autismo_sorri,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoincomodadabarulho') AS triagem_autismo_incomodada_barulho,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoanda') AS triagem_autismo_anda,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoolhanosolhos') AS triagem_autismo_olha_nos_olhos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoimita') AS triagem_autismo_imita,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoolhando') AS triagem_autismo_olhando,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismofazolhar') AS triagem_autismo_faz_olhar,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismocompreendepedido') AS triagem_autismo_compreende_pedido,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoolhacomOsente') AS triagem_autismo_olha_como_sente,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismogostaatividades') AS triagem_autismo_gosta_atividades,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemautismoclassificacao') AS triagem_autismo_classificacao,

    loaded_at,
    data_particao
  FROM bruto_atendimento
),

saude_crianca_dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY loaded_at DESC) AS rn
  FROM saude_crianca_extraida
)

SELECT * EXCEPT (rn)
FROM saude_crianca_dedup
WHERE rn = 1