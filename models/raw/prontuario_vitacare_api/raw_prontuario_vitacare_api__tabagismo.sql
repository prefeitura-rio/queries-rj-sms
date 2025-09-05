{{ config(
    alias="tabagismo",
    materialized="incremental",
    schema="brutos_prontuario_vitacare_api",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "data_particao", "data_type": "date", "granularity": "day"}
) }}

{% set last_30_days = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

WITH bruto_atendimento AS (
  SELECT
    CAST(source_id AS STRING) AS id_prontuario_local,
    CONCAT(NULLIF(CAST(payload_cnes AS STRING), ''), '.', NULLIF(CAST(source_id AS STRING), '')) AS id_prontuario_global,
    CAST(payload_cnes AS STRING) AS id_cnes,
    SAFE_CAST(datalake_loaded_at AS DATETIME) AS loaded_at,
    data,
    DATE(SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.datahora_fim_atendimento') AS DATETIME)) AS data_particao
  FROM {{ source("brutos_prontuario_vitacare_staging_dev", "atendimento_continuo") }}
  WHERE JSON_EXTRACT(data, '$.tabagismo') IS NOT NULL
  AND JSON_EXTRACT(data, '$.tabagismo') != '[]'
  {% if is_incremental() %}
    AND DATE(datalake_loaded_at, 'America/Sao_Paulo') >= {{ last_30_days }}
  {% endif %}
  qualify row_number() over(partition by id_prontuario_global order by datalake_loaded_at desc) = 1
),

tabagismo_extraido AS (
  SELECT
    id_prontuario_global,
    id_prontuario_local,
    id_cnes,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].cdtPeso') AS FLOAT64) AS cdt_peso,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].cdtAltura') AS cdt_altura,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].cdtImc') AS FLOAT64) AS cdt_imc,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].cdtTamax') AS cdt_tamax,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].cdtTamin') AS cdt_tamin,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].cdtIntervencoes') AS cdt_intervencoes,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].cdtIntervencoesObs') AS cdt_intervencoesobs,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].cdtPerimAbdominal') AS cdt_perimabdominal,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].monoxidoCarbono') AS monoxido_carbono,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tempoPrimCig') AS tempo_prim_cig,

    CASE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fumarLocaisProib')) = 'sim' THEN TRUE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fumarLocaisProib')) = 'não' THEN FALSE
        ELSE NULL
    END AS fumar_locais_proib,

    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].melhorCigDia') AS melhor_cig_dia,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].numCigDia') AS num_cig_dia,

    CASE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fumaManha')) = 'sim' THEN TRUE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fumaManha')) = 'não' THEN FALSE
        ELSE NULL
    END AS fuma_manha,

    CASE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fumaDoente')) = 'sim' THEN TRUE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fumaDoente')) = 'não' THEN FALSE
        ELSE NULL
    END AS fuma_doente,

    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fagerstronGrauDepend') AS fagerstrom_grau_depend,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fumanteFasesMotivacionais') AS fumante_fases_motivacionais,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].fumanteNotasObservacoesTxt') AS fumante_notas_observacoes_txt,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoTipoUtilizado') AS tabaco_tipo_utilizado,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoTipoUtilizadoQual') AS tabaco_tipo_utilizado_qual,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoParticipouTratamento') AS tabaco_participou_tratamento,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoEncontro') AS tabaco_encontro,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoEncontroData') AS DATETIME) AS tabaco_encontro_data,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoSituacaoPacienteFase1') AS tabaco_situacao_paciente_fase1,

    CASE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoApoioMedicamento')) = 'sim' THEN TRUE
        WHEN LOWER(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoApoioMedicamento')) = 'não' THEN FALSE
        ELSE NULL
    END AS tabaco_apoio_medicamento,

    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoApoioMedicamentoData') AS DATETIME) AS tabaco_apoio_medicamento_data,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoTipoMedicacao') AS tabaco_tipo_medicacao,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoManutencao') AS tabaco_manutencao,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoManutencaoData') AS DATETIME) AS tabaco_manutencao_data,
    JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoSituacaoPacienteFase2') AS tabaco_situacao_paciente_fase2,
    SAFE_CAST(JSON_EXTRACT_SCALAR(data, '$.tabagismo[0].tabacoApoioMedicamentoDataFim') AS DATETIME) AS tabaco_apoio_medicamento_data_fim,

    loaded_at,
    data_particao

  FROM bruto_atendimento
),

tabagismo_dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_prontuario_global ORDER BY loaded_at DESC) AS rn
  FROM tabagismo_extraido
)

SELECT * EXCEPT (rn)
FROM tabagismo_dedup
WHERE rn = 1