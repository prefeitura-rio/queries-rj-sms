{{
    config(
        alias="eventos_obstetricos_inicio_fim",
        schema="intermediario_historico_clinico",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_evento_obstetrico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        cluster_by=["fonte", "tipo_evento", "cpf", "id_hci"],
        tags=["daily"],
        meta={"owner": "karen"}
    )
}}

{% set janela_incremental = "DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 7 DAY)" %}

WITH
-- Episodios assistenciais com CIDs de gestacao, preservando a fonte do prontuario original
    hci_episodios AS (
        SELECT
            CAST(e.id_hci AS STRING) AS id_hci,
            CAST(e.paciente.id_paciente AS STRING) AS id_paciente,
            REGEXP_REPLACE(CAST(e.paciente_cpf AS STRING), r'\D', '') AS cpf,
            CASE
                WHEN e.prontuario.fornecedor IS NULL THEN NULL
                WHEN LOWER(e.prontuario.fornecedor) = 'vitacare' THEN 'vitacare'
                WHEN LOWER(e.prontuario.fornecedor) = 'vitai' THEN 'vitai'
                WHEN LOWER(e.prontuario.fornecedor) = 'mv' THEN 'mv'
                WHEN LOWER(e.prontuario.fornecedor) = 'prontuario' THEN 'prontuaRio'
                WHEN LOWER(e.prontuario.fornecedor) = 'pcsm' THEN 'pcsm'
                WHEN LOWER(e.prontuario.fornecedor) = 'sarah' THEN 'sarah'
                ELSE LOWER(e.prontuario.fornecedor)
            END AS fonte,
            c.id AS cid,
            c.situacao AS situacao_cid,
            SAFE.PARSE_DATE('%Y-%m-%d', SUBSTR(c.data_diagnostico, 1, 10)) AS data_evento
        FROM {{ ref("mart_historico_clinico__episodio") }} AS e
        LEFT JOIN UNNEST(e.condicoes) AS c
        WHERE
            c.data_diagnostico IS NOT NULL
            AND c.data_diagnostico != ''
            AND c.situacao IN ('ATIVO', 'RESOLVIDO')
            AND (
                c.id = 'Z321'
                OR c.id LIKE 'Z34%'
                OR c.id LIKE 'Z35%'
            )
            {% if is_incremental() %}
                AND DATE(e.metadados.imported_at) >= {{ janela_incremental }}
            {% endif %}
    ),

-- Transforma CID ativo/resolvido em eventos de inicio/fim de gestacao
    eventos_hci AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "fonte",
                "id_hci",
                "cid",
                "situacao_cid",
                "CAST(data_evento AS STRING)"
            ]) }} AS id_evento_obstetrico,
            id_paciente,
            cpf,
            id_hci,
            fonte,
            id_hci AS id_evento_origem,
            data_evento,
            CASE
                WHEN situacao_cid = 'ATIVO' THEN 'inicio_gestacao'
                WHEN situacao_cid = 'RESOLVIDO' THEN 'fim_gestacao'
            END AS tipo_evento,
            'cid_gestacao' AS subtipo_evento,
            IF(situacao_cid = 'ATIVO', data_evento, NULL) AS data_inicio_gestacao,
            IF(situacao_cid = 'RESOLVIDO', data_evento, NULL) AS data_fim_gestacao,
            CAST(NULL AS DATE) AS data_parto,
            CAST(NULL AS DATE) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            CAST(NULL AS INT64) AS idade_gestacional_dias,
            cid,
            CAST(NULL AS STRING) AS procedimento_codigo,
            CAST(NULL AS STRING) AS procedimento_descricao,
            CAST(NULL AS DATETIME) AS loaded_at,
            data_evento AS data_particao
        FROM hci_episodios
        WHERE data_evento IS NOT NULL
    ),

-- Resgata campos do atendimento de Pre-natal enviados pelo backup da Vitacare para obter CPF e data
    vitacare_historico_prenatal AS (
        SELECT
            'vitacare' AS fonte,
            p.id_prontuario_global AS id_evento_origem,
            CAST(NULL AS STRING) AS id_paciente,
            REGEXP_REPLACE(CAST(a.patient_cpf AS STRING), r'\D', '') AS cpf,
            CAST(a.id_prontuario_global AS STRING) AS id_hci,
            DATE(a.datahora_fim_atendimento) AS data_atendimento,
            p.pconsulta_idade_gestacional,
            p.pconsulta_idade_gestacional_dias,
            p.primeira_consulta_idade_gestacional,
            p.primeira_consulta_idade_gestacional_dias,
            p.pconsulta_dpp1,
            p.sm_puerperio_data,
            p.sm_tipo_parto,
            p.sm_local_parto,
            p.gestante_puerperio_num_recem_nascidos,
            p.puerperio_data_aborto,
            p.puerperio_tipo_aborto,
            p.puerperio_idade_gestacao,
            CAST(p.loaded_at AS DATETIME) AS loaded_at,
            p.data_particao
        FROM {{ ref("raw_prontuario_vitacare_historico__prenatal") }} AS p
        LEFT JOIN {{ ref("raw_prontuario_vitacare_historico__acto") }} AS a
            ON p.id_prontuario_global = a.id_prontuario_global
        {% if is_incremental() %}
            WHERE p.data_particao >= {{ janela_incremental }}
        {% endif %}
    ),

-- Resgata campos do atendimento de Pre-natal da API da Vitacare ligado ao acto pelo mesmo id_prontuario_global
    vitacare_api_prenatal AS (
        SELECT
            'vitacare' AS fonte,
            p.id_prontuario_global AS id_evento_origem,
            CAST(NULL AS STRING) AS id_paciente,
            REGEXP_REPLACE(CAST(a.patient_cpf AS STRING), r'\D', '') AS cpf,
            CAST(a.id_prontuario_global AS STRING) AS id_hci,
            DATE(a.datahora_fim_atendimento) AS data_atendimento,
            p.pconsulta_idade_gestacional,
            p.pconsulta_idade_gestacional_dias,
            p.primeira_consulta_idade_gestacional,
            p.primeira_consulta_idade_gestacional_dias,
            p.pconsulta_dpp1,
            p.smrpuerperiodata AS sm_puerperio_data,
            p.smtipoparto AS sm_tipo_parto,
            p.smlocalparto AS sm_local_parto,
            p.gestantepuerperionumrecemnascidos AS gestante_puerperio_num_recem_nascidos,
            p.puerperiodataaborto AS puerperio_data_aborto,
            p.puerperiotipoaborto AS puerperio_tipo_aborto,
            p.puerperioidadegestacao AS puerperio_idade_gestacao,
            CAST(p.loaded_at AS DATETIME) AS loaded_at,
            p.data_particao
        FROM {{ ref("raw_prontuario_vitacare_api__prenatal") }} AS p
        LEFT JOIN {{ ref("raw_prontuario_vitacare_api__acto") }} AS a
            ON p.id_prontuario_global = a.id_prontuario_global
        {% if is_incremental() %}
            WHERE DATE(p.loaded_at) >= {{ janela_incremental }}
        {% endif %}
    ),

-- Junta dados de pre natal historico + API da vitacare
    vitacare_prenatal AS (
        SELECT * FROM vitacare_historico_prenatal
        UNION ALL
        SELECT * FROM vitacare_api_prenatal
    ),

-- Calcula a idade gestacional total combinando semanas completas + dias adicionais
    vitacare_prenatal_calculado AS (
        SELECT
            *,
            COALESCE(
                CASE
                    WHEN pconsulta_idade_gestacional BETWEEN 0 AND 45
                        AND (
                            pconsulta_idade_gestacional_dias IS NULL
                            OR pconsulta_idade_gestacional_dias BETWEEN 0 AND 6
                        )
                        THEN CAST(
                            (pconsulta_idade_gestacional * 7)
                            + COALESCE(pconsulta_idade_gestacional_dias, 0)
                            AS INT64
                        )
                END,
                CASE
                    WHEN primeira_consulta_idade_gestacional BETWEEN 0 AND 45
                        AND (
                            primeira_consulta_idade_gestacional_dias IS NULL
                            OR primeira_consulta_idade_gestacional_dias BETWEEN 0 AND 6
                        )
                        THEN CAST(
                            (primeira_consulta_idade_gestacional * 7)
                            + COALESCE(primeira_consulta_idade_gestacional_dias, 0)
                            AS INT64
                        )
                END
            ) AS idade_gestacional_total_dias
        FROM vitacare_prenatal
    ),

-- Evidencia gestacao atual por idade gestacional ou DPP registrada no pre-natal
    eventos_vitacare_inicio AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "fonte",
                "id_evento_origem",
                "'inicio_gestacao'",
                "CAST(data_atendimento AS STRING)"
            ]) }} AS id_evento_obstetrico,
            id_paciente,
            NULLIF(cpf, '') AS cpf,
            id_hci,
            fonte,
            id_evento_origem,
            data_atendimento AS data_evento,
            'inicio_gestacao' AS tipo_evento,
            'prenatal' AS subtipo_evento,
            -- Quando ha idade gestacional, estima a DUM pela data do atendimento
            CASE
                WHEN idade_gestacional_total_dias BETWEEN 0 AND 315
                    THEN DATE_SUB(
                        data_atendimento,
                        INTERVAL idade_gestacional_total_dias DAY
                    )
            END AS data_inicio_gestacao,
            CAST(NULL AS DATE) AS data_fim_gestacao,
            CAST(NULL AS DATE) AS data_parto,
            CAST(NULL AS DATE) AS data_puerperio,
            -- DPP pode chegar em formatos diferentes entre API e historico
            COALESCE(
                SAFE_CAST(pconsulta_dpp1 AS DATE),
                SAFE.PARSE_DATE('%d/%m/%Y', pconsulta_dpp1),
                SAFE.PARSE_DATE('%d/%m/%y', pconsulta_dpp1)
            ) AS dpp,
            idade_gestacional_total_dias AS idade_gestacional_dias,
            CAST(NULL AS STRING) AS cid,
            CAST(NULL AS STRING) AS procedimento_codigo,
            CAST(NULL AS STRING) AS procedimento_descricao,
            loaded_at,
            data_particao
        FROM vitacare_prenatal_calculado
        WHERE
            data_atendimento IS NOT NULL
            AND (
                idade_gestacional_total_dias BETWEEN 0 AND 315
                OR COALESCE(
                    SAFE_CAST(pconsulta_dpp1 AS DATE),
                    SAFE.PARSE_DATE('%d/%m/%Y', pconsulta_dpp1),
                    SAFE.PARSE_DATE('%d/%m/%y', pconsulta_dpp1)
                ) IS NOT NULL
            )
    ),

    eventos AS (
        SELECT * FROM eventos_hci
        UNION ALL
        SELECT * FROM eventos_vitacare_inicio
    ),

    eventos_deduplicados AS (
        SELECT *
        FROM eventos
        WHERE
            data_evento IS NOT NULL
            AND data_evento > DATE '1900-01-01'
            AND data_evento <= CURRENT_DATE('America/Sao_Paulo')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY id_evento_obstetrico
            ORDER BY loaded_at DESC, data_particao DESC
        ) = 1
    )

SELECT
    *
FROM eventos_deduplicados
