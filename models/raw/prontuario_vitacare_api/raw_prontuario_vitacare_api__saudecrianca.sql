{{ config(
    alias="saude_crianca",
    materialized="incremental",
    schema="brutos_prontuario_vitacare_api",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"}
) }}

{% set last_partition = get_last_partition_date(this) %}

WITH bruto_atendimento AS (
  SELECT
    CAST(source_id AS STRING) AS id_prontuario_local,
    CONCAT(NULLIF(CAST(payload_cnes AS STRING), ''), '.', NULLIF(CAST(source_id AS STRING), '')) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME)) AS data_particao,
    data,
  FROM {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
  {% if is_incremental() %}
    WHERE DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

saude_crianca_extraida AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    (
      SELECT JSON_EXTRACT_SCALAR(elem, '$')
      FROM UNNEST(COALESCE(JSON_EXTRACT_ARRAY(data, '$.saude_crianca[0].ppneonatal'), [])) AS elem
      LIMIT 1
    ) AS pp_neonatal,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].ppneonatalobs') AS pp_neonatal_obs,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].anomaliascongenitas') AS anomalias_congenitas,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].regcrescAltura') AS FLOAT64) AS reg_cresc_altura,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].regcrescPeso') AS FLOAT64) AS reg_cresc_peso,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].observacoes') AS observacoes,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].regcrescImc') AS FLOAT64) AS reg_cresc_imc,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].rastreios') AS rastreios,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].anamaliascongobs') AS anamalias_cong_obs,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].registocrescimentioobs') AS registocrescimentio_obs,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].tipoparto') AS tipo_parto,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].pesonascer') AS FLOAT64) AS peso_nascer,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].comprimento') AS FLOAT64) AS comprimento,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].perimetrocefalico') AS FLOAT64) AS perimetro_cefalico,
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
    JSON_EXTRACT(data, '$.saude_crianca[0].testePezinhoDoencas') AS teste_pezinho_doencas_json,
    JSON_EXTRACT(data, '$.saude_crianca[0].reflexoVermelho') AS reflexo_vermelho_json,
    JSON_EXTRACT(data, '$.saude_crianca[0].testeOrelhinha') AS teste_orelhinha_json,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].periodoPreNatal') AS periodo_prenatal,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverPeriodoTemporal') AS denver_periodo_temporal,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverPostura') AS denver_postura,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverObservarosto') AS denver_observa_rosto,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverReageSom') AS denver_reage_som,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverElevaCabeca') AS denver_eleva_cabeca,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverSorriso') AS denver_sorriso,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverAbreMaos') AS denver_abre_maos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverEmiteSons') AS denver_emite_sons,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverMovimentos') AS denver_movimentos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverContatoSocial') AS denver_contato_social,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverSeguraObjetos') AS denver_segura_objetos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverEmiteSons24Meses') AS denver_emite_sons_24_meses,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverBrucoLevantaCabeca') AS denver_bruco_levanta_cabeca,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverBuscaObjectos') AS denver_busca_objetos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverLevaObjectos') AS denver_leva_objetos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverLocalizaSom') AS denver_localiza_som,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverMudaPosicaoRola') AS denver_muda_posicao_rola,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverBrincaEscondeAchou') AS denver_brinca_esconde_achou,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverTransfereObjectos') AS denver_transfere_objetos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverDuplicaSilabas') AS denver_duplica_silabas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverSentaSemApoio') AS denver_senta_sem_apoio,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverImitaGestos') AS denver_imita_gestos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverFazPinca') AS denver_faz_pinca,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverProduzJargao') AS denver_produz_jargao,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverAndaComApoio') AS denver_anda_com_apoio,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverMostraOQueQuer') AS denver_mostra_o_que_quer,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverColocaBlocosCaneca') AS denver_coloca_blocos_caneca,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverDizUmaPalavra') AS denver_diz_uma_palavra,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverAndaParaTras') AS denver_anda_para_tras,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverTiraRoupa') AS denver_tira_roupa,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverConstroiTorre3Cubos') AS denver_constroi_torre_3_cubos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverAponta2Figuras') AS denver_aponta_2_figuras,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverChutaBola') AS denver_chuta_bola,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverBrincaComOutrasCriancas') AS denver_brinca_com_outras_criancas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverImitaLinhaVertical') AS denver_imita_linha_vertical,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverReconhece2Acoes') AS denver_reconhece_2_acoes,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].denverArremessaBola') AS denver_arremessa_bola,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].rastreiostestepezinhoobservacoes') AS rastreios_testepezinho_observacoes,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].rastreiosreflexovermelhoobservacoes') AS rastreios_reflexovermelho_observacoes,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].rastreiostesteorelhinhaobservacoes') AS rastreios_testeorelhinha_observacoes,


    (
      SELECT JSON_EXTRACT_SCALAR(elem, '$')
      FROM UNNEST(COALESCE(JSON_EXTRACT_ARRAY(data, '$.saude_crianca[0].tecnicasEspeciais'), [])) AS elem
      LIMIT 1
    ) AS tecnicas_especiais,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].tecnicasEspeciaisOutras') AS tecnicas_especiais_outras,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].sintomasAcuidadeVisual') AS sintomas_acuidade_visual,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].snellenDistanciaEsq') AS snellen_distancia_esq,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].snellenDistanciaDir') AS snellen_distancia_dir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].snellenTipoOptotiposEsq') AS snellen_tipo_optotipos_esq,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].snellenTipoOptotiposDir') AS snellen_tipo_optotipos_dir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].comCorrecaoEsq') AS com_correcao_esq,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].semCorrecaoEsq') AS sem_correcao_esq,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].comCorrecaoDir') AS com_correcao_dir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].semCorrecaoDir') AS sem_correcao_dir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].acuidadeVisualConduta') AS acuidade_visual_conduta,

    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoOlhaObjeto') AS triagem_autismo_olha_objeto,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoPodeSerSurda') AS triagem_autismo_pode_ser_surda,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoBrincaFazDeContas') AS triagem_autismo_brinca_faz_de_contas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoSubirNasCoisas') AS triagem_autismo_subir_nas_coisas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoMovimentosEstranhos') AS triagem_autismo_movimentos_estranhos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoApontaODedoPedir') AS triagem_autismo_aponta_dedo_pedir,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoApontaODedoMostrar') AS triagem_autismo_aponta_dedo_mostrar,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoInteresseOutrasCriancas') AS triagem_autismo_interesse_outras_criancas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoTrazCoisas') AS triagem_autismo_traz_coisas,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoRespondePeloNome') AS triagem_autismo_responde_pelo_nome,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoScore') AS FLOAT64) AS triagem_autismo_score,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoSorri') AS triagem_autismo_sorri,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoIncomodadaBarulho') AS triagem_autismo_incomodada_barulho,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoAnda') AS triagem_autismo_anda,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoOlhaNosOlhos') AS triagem_autismo_olha_nos_olhos,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoImita') AS triagem_autismo_imita,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoOlhando') AS triagem_autismo_olhando,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoFazOlhar') AS triagem_autismo_faz_olhar,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoCompreendePedido') AS triagem_autismo_compreende_pedido,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoOlhaComoSente') AS triagem_autismo_olha_como_sente,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoGostaAtividades') AS triagem_autismo_gosta_atividades,
    JSON_EXTRACT_SCALAR(data, '$.saude_crianca[0].triagemAutismoClassificacao') AS triagem_autismo_classificacao,

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