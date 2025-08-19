{{ config(
    alias="tuberculose",
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
    data,
    DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME)) AS data_particao
  FROM {{ source("brutos_prontuario_vitacare_staging", "atendimento_continuo") }}
  {% if is_incremental() %}
    WHERE DATE(datalake_loaded_at, 'America/Sao_Paulo') >= DATE('{{ last_partition }}')
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

tuberculose_extraida AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].dataInicioTrat') AS DATETIME) AS datainiciotrat,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].idadeInicioTrat') AS INT64) AS idadeiniciotrat,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].pesoInicioTrat') AS NUMERIC) AS pesoiniciotrat,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].contatoTB') AS contatotb,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].tipoEntrada') AS tipoentrada,

    JSON_EXTRACT(data, '$.tuberculose[0].forma') AS forma_json,  
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].baciloEscarro') AS baciloescarro_raw, 
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].culturaEscarro') AS culturaescarro_raw, 

    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].formaTratamento') AS formatratamento,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].drogasUtilizadas') AS drogasutilizadas,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].tipoPrograma') AS tipoprograma,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].numSinan') AS NUMERIC) AS numsinan,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].hiv') AS hiv,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].comunicaTB') AS comunicatb,

    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].raioXTorax') AS raioxtorax_raw,  
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].dataInicioRegistro') AS DATETIME) AS datainicioregistro,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].resultadoRaioX') AS resultadoraiox,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].histBiopsiaPleural') AS histbiopsiapleural_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].histBiopsiaGanglionar') AS histbiopsiaganglionar_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].histOutrosTecidos') AS histoutrostecidos_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].sintomatologiaRespiratoriaPrevia') AS sintomatologiarespiratoriaprevia,
    JSON_EXTRACT(data, '$.tuberculose[0].sintomatologiaRespiratoriaDiagnosticos') AS sintomatologiarespiratoriadiagnosticos_json,

    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].testeSensibilidadeCultura') AS testesensibilidadecultura_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].testeMolecularTuberculose') AS testemoleculartuberculose,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].igra') AS igra_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].lfLam') AS lflam_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].lpa') AS lpa_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].culturaOutros') AS culturaoutros_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].culturaOutrosResultado') AS culturaoutrosresultado_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].outrosExames') AS outrosexames,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].pesoAcompanhamentoMensal') AS NUMERIC) AS pesoacompanhamentomensal,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].resultadoBaciloEscarro') AS resultadobaciloescarro,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].faseTratamento') AS fasetratamento,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].acompanhamentoMensalObs') AS acompanhamentomensalobs,

    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].hivPositivoNegativoMensal') AS hivpositivonegativomensal_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].raioXToraxMensal') AS raioxtoraxmensal_raw,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].resultadoRaioXToraxAcompanhamentoMensal') AS resultadoraioxtoraxacompanhamentomensal,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].idadeExclTrat') AS INT64) AS idadeexcltrat,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].dataExclusao') AS DATETIME) AS dataexclusao,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].pesoEncerramento') AS NUMERIC) AS pesoencerramento,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].resultadoBaciloEscarroEncerramento') AS resultadobaciloescarroencerramento,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].motivoEncerramento') AS motivoencerramento,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].hivPositivoNegativoEncerramento') AS hivpositivonegativoencerramento,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].raioXToraxEncerramento') AS raioxtoraxencerramento,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].resultadoRaioXToraxEncerramento') AS resultadoraioxtoraxencerramento,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].dataInicioTratamentoLatente') AS DATETIME) AS datainiciotratamentolatente,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].raioXToraxTratamentoLatente') AS raioxtoraxtratamentolatente,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].ppdTratamentoLatente') AS ppdtratamentolatente,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].ppdDataTratamentoLatente') AS DATETIME) AS ppddatatratamentolatente,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].indicacaoTratamentoLatente') AS indicacaotratamentolatente,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].dataEncerramentoTratamentoLatente') AS DATETIME) AS dataencerramentotratamentolatente,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].resultadoRaioXToraxTratamentoLatente') AS resultadoraioxtoraxtratamentolatente,

    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].esquemaIltb') AS esquemailtb,
    JSON_EXTRACT_SCALAR(data, '$.tuberculose[0].observacoesTuber') AS observacoestuber,

    loaded_at,
    data_particao

  FROM bruto_atendimento
),

tuberculose_dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY loaded_at DESC) AS rn
  FROM tuberculose_extraida
)

SELECT * EXCEPT (rn)
FROM tuberculose_dedup
WHERE rn = 1