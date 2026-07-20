{{
    config(
        alias="eventos_obstetricos_parto",
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
-- Vitai identifica puerperio por nascimento/RN vinculado a mae
    eventos_vitai_parto AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'vitai'",
                "gid",
                "'parto'",
                "CAST(parto_datahora AS STRING)"
            ]) }} AS id_evento_obstetrico,
            CAST(id_paciente_rede_mae AS STRING) AS id_paciente,
            CAST(NULL AS STRING) AS cpf,
            CAST(gid_boletim_mae AS STRING) AS id_hci,
            'vitai' AS fonte,
            CAST(gid AS STRING) AS id_evento_origem,
            DATE(parto_datahora) AS data_evento,
            'parto' AS tipo_evento,
            'nascimento_rn' AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            DATE(parto_datahora) AS data_fim_gestacao,
            DATE(parto_datahora) AS data_parto,
            CAST(NULL AS DATE) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            SAFE_CAST(gestacao_semanas AS INT64) * 7 AS idade_gestacional_dias,
            CAST(NULL AS STRING) AS cid,
            CAST(NULL AS STRING) AS procedimento_codigo,
            CAST(NULL AS STRING) AS procedimento_descricao,
            CAST(imported_at AS DATETIME) AS loaded_at,
            data_particao
        FROM {{ ref("raw_prontuario_vitai__dtw__recem_nascido") }}
        WHERE
            parto_datahora IS NOT NULL
            {% if is_incremental() %}
                AND DATE(imported_at) >= {{ janela_incremental }}
            {% endif %}
    ),

-- Vitai cirurgia traz procedimentos obstetricos estruturados com codigo SIGTAP
    eventos_vitai_cirurgia_procedimento AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'vitai_cirurgia'",
                "gid",
                "procedimento_codigo_normalizado",
                "tipo_evento",
                "CAST(data_evento AS STRING)"
            ]) }} AS id_evento_obstetrico,
            CAST(gid_paciente AS STRING) AS id_paciente,
            CAST(NULL AS STRING) AS cpf,
            CAST(gid_boletim AS STRING) AS id_hci,
            'vitai' AS fonte,
            CAST(gid AS STRING) AS id_evento_origem,
            data_evento,
            tipo_evento,
            CASE
                WHEN tipo_evento = 'parto' THEN 'procedimento_parto'
                WHEN tipo_evento = 'aborto' THEN 'procedimento_aborto'
                WHEN tipo_evento = 'avaliacao_puerperal' THEN 'procedimento_puerperio'
            END AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            IF(tipo_evento IN ('parto', 'aborto'), data_evento, NULL) AS data_fim_gestacao,
            IF(tipo_evento = 'parto', data_evento, NULL) AS data_parto,
            IF(tipo_evento = 'avaliacao_puerperal', data_evento, NULL) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            CAST(NULL AS INT64) AS idade_gestacional_dias,
            CAST(NULL AS STRING) AS cid,
            procedimento_codigo_normalizado AS procedimento_codigo,
            procedimento_nome AS procedimento_descricao,
            CAST(imported_at AS DATETIME) AS loaded_at,
            data_particao
        FROM (
            SELECT
                *,
                LPAD(CAST(procedimento_codigo AS STRING), 10, '0') AS procedimento_codigo_normalizado,
                SAFE_CAST(cirurgia_data AS DATE) AS data_evento,
                CASE
                    WHEN LPAD(CAST(procedimento_codigo AS STRING), 10, '0') IN (
                        '0310010012',
                        '0310010039',
                        '0310010047',
                        '0310010055',
                        '0411010026',
                        '0411010034',
                        '0411010042'
                    )
                        THEN 'parto'
                    WHEN LPAD(CAST(procedimento_codigo AS STRING), 10, '0') IN (
                        '0409060070',
                        '0411020013'
                    )
                        THEN 'aborto'
                    WHEN LPAD(CAST(procedimento_codigo AS STRING), 10, '0') IN (
                        '0303100010',
                        '0411010069',
                        '0411020030'
                    )
                        THEN 'avaliacao_puerperal'
                END AS tipo_evento
            FROM {{ ref("raw_prontuario_vitai__cirurgia") }}
            WHERE
                status = 'REALIZADA'
                AND LPAD(CAST(procedimento_codigo AS STRING), 10, '0') IN (
                    '0303100010',
                    '0310010012',
                    '0310010039',
                    '0310010047',
                    '0310010055',
                    '0409060070',
                    '0411010026',
                    '0411010034',
                    '0411010042',
                    '0411010069',
                    '0411020013',
                    '0411020030'
                )
                {% if is_incremental() %}
                    AND data_particao >= {{ janela_incremental }}
                {% endif %}
        )
        WHERE tipo_evento IS NOT NULL
    ),

-- Vitai internacao traz procedimento principal da internacao como evidencia obstetrica adicional
    eventos_vitai_internacao_procedimento AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'vitai_internacao'",
                "gid",
                "procedimento_codigo_normalizado",
                "tipo_evento",
                "CAST(data_evento AS STRING)"
            ]) }} AS id_evento_obstetrico,
            CAST(gid_paciente AS STRING) AS id_paciente,
            NULLIF(REGEXP_REPLACE(CAST(cpf AS STRING), r'\D', ''), '') AS cpf,
            CAST(gid_boletim AS STRING) AS id_hci,
            'vitai' AS fonte,
            CAST(gid AS STRING) AS id_evento_origem,
            data_evento,
            tipo_evento,
            CASE
                WHEN tipo_evento = 'parto' THEN 'procedimento_parto'
                WHEN tipo_evento = 'aborto' THEN 'procedimento_aborto'
                WHEN tipo_evento = 'avaliacao_puerperal' THEN 'procedimento_puerperio'
            END AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            IF(tipo_evento IN ('parto', 'aborto'), data_evento, NULL) AS data_fim_gestacao,
            IF(tipo_evento = 'parto', data_evento, NULL) AS data_parto,
            IF(tipo_evento = 'avaliacao_puerperal', data_evento, NULL) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            CAST(NULL AS INT64) AS idade_gestacional_dias,
            CAST(NULL AS STRING) AS cid,
            procedimento_codigo_normalizado AS procedimento_codigo,
            procedimento_nome AS procedimento_descricao,
            CAST(imported_at AS DATETIME) AS loaded_at,
            data_particao
        FROM (
            SELECT
                *,
                LPAD(CAST(id_procedimento AS STRING), 10, '0') AS procedimento_codigo_normalizado,
                DATE(COALESCE(saida_data, internacao_data)) AS data_evento,
                CASE
                    WHEN LPAD(CAST(id_procedimento AS STRING), 10, '0') IN (
                        '0310010012',
                        '0310010039',
                        '0310010047',
                        '0310010055',
                        '0411010026',
                        '0411010034',
                        '0411010042'
                    )
                        THEN 'parto'
                    WHEN LPAD(CAST(id_procedimento AS STRING), 10, '0') IN (
                        '0409060070',
                        '0411020013'
                    )
                        THEN 'aborto'
                    WHEN LPAD(CAST(id_procedimento AS STRING), 10, '0') IN (
                        '0303100010',
                        '0411010069',
                        '0411020030'
                    )
                        THEN 'avaliacao_puerperal'
                END AS tipo_evento
            FROM {{ ref("raw_prontuario_vitai__internacao") }}
            WHERE
                LPAD(CAST(id_procedimento AS STRING), 10, '0') IN (
                    '0303100010',
                    '0310010012',
                    '0310010039',
                    '0310010047',
                    '0310010055',
                    '0409060070',
                    '0411010026',
                    '0411010034',
                    '0411010042',
                    '0411010069',
                    '0411020013',
                    '0411020030'
                )
                {% if is_incremental() %}
                    AND data_particao >= {{ janela_incremental }}
                {% endif %}
        )
        WHERE tipo_evento IS NOT NULL
    ),

-- MV gestante traz parto estruturado com data e tipo de parto
    eventos_mv_gestante_parto AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'mv_gestante'",
                "id_hci",
                "'parto'",
                "CAST(dt_parto AS STRING)"
            ]) }} AS id_evento_obstetrico,
            CAST(id_paciente AS STRING) AS id_paciente,
            REGEXP_REPLACE(CAST(paciente_cpf AS STRING), r'\D', '') AS cpf,
            CAST(id_hci AS STRING) AS id_hci,
            'mv' AS fonte,
            CAST(id_hci AS STRING) AS id_evento_origem,
            DATE(dt_parto) AS data_evento,
            'parto' AS tipo_evento,
            'registro_parto' AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            DATE(dt_parto) AS data_fim_gestacao,
            DATE(dt_parto) AS data_parto,
            CAST(NULL AS DATE) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            CAST(NULL AS INT64) AS idade_gestacional_dias,
            CAST(NULL AS STRING) AS cid,
            CAST(NULL AS STRING) AS procedimento_codigo,
            CAST(NULL AS STRING) AS procedimento_descricao,
            CAST(loaded_at AS DATETIME) AS loaded_at,
            data_particao
        FROM {{ ref("raw_prontuario_mv__gestante") }}
        WHERE
            dt_parto IS NOT NULL
            {% if is_incremental() %}
                AND data_particao >= {{ janela_incremental }}
            {% endif %}
    ),

-- MV admissao neonatal identifica parto pelo bloco de nascimento/RN
    eventos_mv_admissao_parto AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'mv_admissao'",
                "id_hci",
                "'parto'",
                "CAST(parto_nascimento_datahora AS STRING)"
            ]) }} AS id_evento_obstetrico,
            CAST(NULL AS STRING) AS id_paciente,
            REGEXP_REPLACE(CAST(paciente_cpf AS STRING), r'\D', '') AS cpf,
            CAST(id_hci AS STRING) AS id_hci,
            'mv' AS fonte,
            CAST(id_hci AS STRING) AS id_evento_origem,
            DATE(COALESCE(
                SAFE_CAST(parto_nascimento_datahora AS DATETIME),
                SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', parto_nascimento_datahora)
            )) AS data_evento,
            'parto' AS tipo_evento,
            'registro_parto' AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            DATE(COALESCE(
                SAFE_CAST(parto_nascimento_datahora AS DATETIME),
                SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', parto_nascimento_datahora)
            )) AS data_fim_gestacao,
            DATE(COALESCE(
                SAFE_CAST(parto_nascimento_datahora AS DATETIME),
                SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', parto_nascimento_datahora)
            )) AS data_parto,
            CAST(NULL AS DATE) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            SAFE_CAST(idade_gestacional_semanas AS INT64) * 7 AS idade_gestacional_dias,
            CAST(NULL AS STRING) AS cid,
            CAST(NULL AS STRING) AS procedimento_codigo,
            CAST(NULL AS STRING) AS procedimento_descricao,
            CAST(loaded_at AS DATETIME) AS loaded_at,
            data_particao
        FROM {{ ref("raw_prontuario_mv__admissao") }}
        WHERE
            COALESCE(
                SAFE_CAST(parto_nascimento_datahora AS DATETIME),
                SAFE.PARSE_DATETIME('%Y/%m/%d %H:%M:%S', parto_nascimento_datahora)
            ) IS NOT NULL
            {% if is_incremental() %}
                AND data_particao >= {{ janela_incremental }}
            {% endif %}
    ),

-- MV alta traz procedimentos realizados em texto livre; usa apenas termos obstetricos explicitos
    eventos_mv_alta_procedimento AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'mv_alta'",
                "id_hci",
                "tipo_evento",
                "procedimentos_realizados",
                "CAST(data_evento AS STRING)"
            ]) }} AS id_evento_obstetrico,
            CAST(NULL AS STRING) AS id_paciente,
            NULLIF(REGEXP_REPLACE(CAST(paciente_cpf AS STRING), r'\D', ''), '') AS cpf,
            CAST(id_hci AS STRING) AS id_hci,
            'mv' AS fonte,
            CAST(id_hci AS STRING) AS id_evento_origem,
            data_evento,
            tipo_evento,
            CASE
                WHEN tipo_evento = 'parto' THEN 'procedimento_parto'
                WHEN tipo_evento = 'aborto' THEN 'procedimento_aborto'
                WHEN tipo_evento = 'avaliacao_puerperal' THEN 'procedimento_puerperio'
            END AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            IF(tipo_evento IN ('parto', 'aborto'), data_evento, NULL) AS data_fim_gestacao,
            IF(tipo_evento = 'parto', data_evento, NULL) AS data_parto,
            IF(tipo_evento = 'avaliacao_puerperal', data_evento, NULL) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            CAST(NULL AS INT64) AS idade_gestacional_dias,
            CAST(NULL AS STRING) AS cid,
            CAST(NULL AS STRING) AS procedimento_codigo,
            procedimentos_realizados AS procedimento_descricao,
            CAST(loaded_at AS DATETIME) AS loaded_at,
            data_particao
        FROM (
            SELECT
                *,
                DATE(COALESCE(alta_medica_datahora, alta_datahora_fechamento, atendimento_datahora)) AS data_evento,
                CASE
                    WHEN (
                        REGEXP_CONTAINS(
                            UPPER(procedimentos_realizados),
                            r'\bABORT[A-Z]*\b|\bAMIU\b|ESVAZIAMENTO\s+DE\s+UTERO\s+POS-ABORTO'
                        )
                        OR (
                            REGEXP_CONTAINS(UPPER(procedimentos_realizados), r'\bCURETAGEM\b|\bWINTERCURETAGEM\b')
                            AND NOT REGEXP_CONTAINS(
                                UPPER(procedimentos_realizados),
                                r'PARTO\s+(NORMAL|VAGINAL|CES|CESAR|CESARE|CESARI|PRETERMO)|OPERACAO\s+CESARIANA|CESARIANA|CESAREANA|CESAREO|CESAREA|CESARIA|CESAREANO|FORCEPS'
                            )
                        )
                    )
                        THEN 'aborto'
                    WHEN REGEXP_CONTAINS(UPPER(procedimentos_realizados), r'PUERP')
                        THEN 'avaliacao_puerperal'
                    WHEN REGEXP_CONTAINS(
                        UPPER(procedimentos_realizados),
                        r'PARTO\s+(NORMAL|VAGINAL|CES|CESAR|CESARE|CESARI|PRETERMO)|OPERACAO\s+CESARIANA|CESARIANA|CESAREANA|CESAREO|CESAREA|CESARIA|CESAREANO|FORCEPS|POS\s+PARTO|D[0-9]+\s+POS\s+PARTO'
                    )
                    AND NOT REGEXP_CONTAINS(
                        UPPER(procedimentos_realizados),
                        r'FALSO TRABALHO DE PARTO|GESTACAO EM CURSO'
                    )
                        THEN 'parto'
                END AS tipo_evento
            FROM {{ ref("raw_prontuario_mv__alta") }}
            WHERE
                procedimentos_realizados IS NOT NULL
                {% if is_incremental() %}
                    AND data_particao >= {{ janela_incremental }}
                {% endif %}
        )
        WHERE tipo_evento IS NOT NULL
    ),

-- SISARE: dados de internacao usados como referencia temporal auxiliar
    sisare_internacoes AS (
        SELECT
            SAFE_CAST(id_internacao AS INT64) AS id_internacao,
            dt_entrada,
            dt_saida,
            unidade_atendimento,
            motivo_internacao,
            datalake_loaded_at
        FROM {{ ref("raw_plataforma_subpav_sisare__internacoes") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY SAFE_CAST(id_internacao AS INT64)
            ORDER BY datalake_loaded_at DESC
        ) = 1
    ),

-- SISARE: base enriquecida de gestantes, mantendo a fonte operacional separada dos prontuarios
    sisare_gestantes AS (
        SELECT
            CAST(NULL AS STRING) AS id_paciente,
            NULLIF(REGEXP_REPLACE(CAST(s.cpf AS STRING), r'\D', ''), '') AS cpf,
            CAST(NULL AS STRING) AS id_hci,
            CAST(s.id_gestante AS STRING) AS id_gestante,
            CAST(s.id_paciente AS STRING) AS id_paciente_sisare,
            CAST(s.id_internacao AS STRING) AS id_internacao,
            s.ig,
            s.id_tipo_gravidez,
            s.id_via_parto,
            s.dt_parto,
            s.id_desfecho_internacao,
            s.id_desfecho_gestacao,
            s.desfecho_gestacao,
            s.puerpera,
            i.dt_entrada,
            i.dt_saida,
            i.unidade_atendimento,
            i.motivo_internacao,
            s.created_at,
            s.updated_at,
            CAST(s.datalake_loaded_at AS DATETIME) AS loaded_at,
            DATE(s.datalake_loaded_at) AS data_particao
        FROM {{ ref("int_subpav__sisare_gestantes") }} AS s
        LEFT JOIN sisare_internacoes AS i
            ON i.id_internacao = s.id_internacao
        WHERE
            s.cpf IS NOT NULL
            {% if is_incremental() %}
                AND DATE(s.datalake_loaded_at) >= {{ janela_incremental }}
            {% endif %}
    ),

-- SISARE: parto informado diretamente na gestante
    eventos_sisare_parto AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'sisare'",
                "id_gestante",
                "id_internacao",
                "'parto'",
                "CAST(dt_parto AS STRING)"
            ]) }} AS id_evento_obstetrico,
            id_paciente,
            cpf,
            id_hci,
            'sisare' AS fonte,
            CONCAT(id_gestante, '|', COALESCE(id_internacao, '')) AS id_evento_origem,
            dt_parto AS data_evento,
            'parto' AS tipo_evento,
            'registro_parto' AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            dt_parto AS data_fim_gestacao,
            dt_parto AS data_parto,
            CAST(NULL AS DATE) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            SAFE_CAST(ig AS INT64) * 7 AS idade_gestacional_dias,
            CAST(NULL AS STRING) AS cid,
            CAST(NULL AS STRING) AS procedimento_codigo,
            CAST(NULL AS STRING) AS procedimento_descricao,
            loaded_at,
            data_particao
        FROM sisare_gestantes
        WHERE dt_parto IS NOT NULL
    ),

-- Dicionario de procedimentos para interpretar codigos do ProntuaRio
    procedimentos AS (
        SELECT
            LPAD(CAST(codigo_procedimento AS STRING), 10, '0') AS codigo_procedimento,
            nome_procedimento
        FROM {{ ref("raw_gdb_sih__tu_procedimento") }}
        WHERE codigo_procedimento IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY LPAD(CAST(codigo_procedimento AS STRING), 10, '0')
            ORDER BY data_particao DESC, data_carga DESC
        ) = 1
    ),

-- Normaliza os quatro campos de procedimento do ProntuaRio em linhas
    prontuario_procedimentos AS (
        SELECT
            ia.*,
            proc.campo_procedimento,
            proc.codigo_procedimento,
            p.nome_procedimento
        FROM {{ ref("raw_prontuario_prontuaRio__internacao_alta") }} AS ia,
            UNNEST([
                STRUCT('procsol' AS campo_procedimento, ia.procsol AS codigo_procedimento),
                STRUCT('procreal', ia.procreal),
                STRUCT('procprin', ia.procprin),
                STRUCT('procsec', ia.procsec)
            ]) AS proc
        LEFT JOIN procedimentos AS p
            ON LPAD(CAST(proc.codigo_procedimento AS STRING), 10, '0') = p.codigo_procedimento
        WHERE
            proc.codigo_procedimento IS NOT NULL
            AND TRIM(CAST(proc.codigo_procedimento AS STRING)) NOT IN ('', '0', '00', '000', '0000', '000000', '0000000000')
            {% if is_incremental() %}
                AND ia.data_particao >= {{ janela_incremental }}
            {% endif %}
    ),

-- ProntuaRio: identifica o CID obstetrico que disparou a regra de parto
    prontuario_altas_cid_parto AS (
        SELECT
            *,
            CASE
                WHEN REGEXP_CONTAINS(COALESCE(codigo_cid10, ''), r'^(O8[0-9]|O9[0-2]|Z37)')
                    THEN codigo_cid10
                WHEN REGEXP_CONTAINS(COALESCE(codigo_cid10_secundario, ''), r'^(O8[0-9]|O9[0-2]|Z37)')
                    THEN codigo_cid10_secundario
            END AS cid_parto
        FROM {{ ref("raw_prontuario_prontuaRio__internacao_alta") }}
        WHERE
            alta_data IS NOT NULL
            {% if is_incremental() %}
                AND data_particao >= {{ janela_incremental }}
            {% endif %}
            AND (
                REGEXP_CONTAINS(COALESCE(codigo_cid10, ''), r'^(O8[0-9]|O9[0-2]|Z37)')
                OR REGEXP_CONTAINS(COALESCE(codigo_cid10_secundario, ''), r'^(O8[0-9]|O9[0-2]|Z37)')
            )
    ),

-- ProntuaRio: usa alta + CID de parto/puerperio como evidencia indireta
    eventos_prontuario_cid AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'prontuaRio'",
                "CAST(gid_prontuario AS STRING)",
                "cid_parto",
                "CAST(alta_data AS STRING)"
            ]) }} AS id_evento_obstetrico,
            CAST(NULL AS STRING) AS id_paciente,
            CAST(NULL AS STRING) AS cpf,
            CAST(gid_prontuario AS STRING) AS id_hci,
            'prontuaRio' AS fonte,
            CAST(gid_prontuario AS STRING) AS id_evento_origem,
            alta_data AS data_evento,
            'parto' AS tipo_evento,
            'cid_parto' AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            alta_data AS data_fim_gestacao,
            alta_data AS data_parto,
            CAST(NULL AS DATE) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            CAST(NULL AS INT64) AS idade_gestacional_dias,
            cid_parto AS cid,
            CAST(NULL AS STRING) AS procedimento_codigo,
            CAST(NULL AS STRING) AS procedimento_descricao,
            CAST(loaded_at AS DATETIME) AS loaded_at,
            data_particao
        FROM prontuario_altas_cid_parto
    ),

-- ProntuaRio: usa alta + procedimento descrito como parto; exclui aborto/intercorrencia
    eventos_prontuario_procedimento AS (
        SELECT
            {{ dbt_utils.generate_surrogate_key([
                "'prontuaRio'",
                "CAST(gid_prontuario AS STRING)",
                "campo_procedimento",
                "CAST(codigo_procedimento AS STRING)",
                "CAST(alta_data AS STRING)"
            ]) }} AS id_evento_obstetrico,
            CAST(NULL AS STRING) AS id_paciente,
            CAST(NULL AS STRING) AS cpf,
            CAST(gid_prontuario AS STRING) AS id_hci,
            'prontuaRio' AS fonte,
            CAST(gid_prontuario AS STRING) AS id_evento_origem,
            alta_data AS data_evento,
            'parto' AS tipo_evento,
            'procedimento_parto' AS subtipo_evento,
            CAST(NULL AS DATE) AS data_inicio_gestacao,
            alta_data AS data_fim_gestacao,
            alta_data AS data_parto,
            CAST(NULL AS DATE) AS data_puerperio,
            CAST(NULL AS DATE) AS dpp,
            CAST(NULL AS INT64) AS idade_gestacional_dias,
            COALESCE(codigo_cid10, codigo_cid10_secundario) AS cid,
            LPAD(CAST(codigo_procedimento AS STRING), 10, '0') AS procedimento_codigo,
            nome_procedimento AS procedimento_descricao,
            CAST(loaded_at AS DATETIME) AS loaded_at,
            data_particao
        FROM prontuario_procedimentos
        WHERE
            alta_data IS NOT NULL
            AND REGEXP_CONTAINS(UPPER(COALESCE(nome_procedimento, '')), r'\bPARTO\b')
            AND NOT REGEXP_CONTAINS(UPPER(COALESCE(nome_procedimento, '')), r'\bABORT[A-Z]*\b|\bINTERCORREN[A-Z]*\b')
    ),

    eventos AS (
        SELECT * FROM eventos_vitai_parto
        UNION ALL
        SELECT * FROM eventos_vitai_cirurgia_procedimento WHERE tipo_evento = 'parto'
        UNION ALL
        SELECT * FROM eventos_vitai_internacao_procedimento WHERE tipo_evento = 'parto'
        UNION ALL
        SELECT * FROM eventos_mv_gestante_parto
        UNION ALL
        SELECT * FROM eventos_mv_admissao_parto
        UNION ALL
        SELECT * FROM eventos_mv_alta_procedimento WHERE tipo_evento = 'parto'
        UNION ALL
        SELECT * FROM eventos_sisare_parto
        UNION ALL
        SELECT * FROM eventos_prontuario_cid
        UNION ALL
        SELECT * FROM eventos_prontuario_procedimento
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
