{{
    config(
        schema="intermediario_historico_clinico",
        alias="eventos_obstetricos",
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

WITH
    eventos AS (
        SELECT * FROM {{ ref("int_historico_clinico__gestacoes__eventos_inicio_fim") }}

        UNION ALL

        SELECT * FROM {{ ref("int_historico_clinico__gestacoes__eventos_parto") }}

        UNION ALL

        SELECT * FROM {{ ref("int_historico_clinico__gestacoes__eventos_aborto") }}

        UNION ALL

        SELECT * FROM {{ ref("int_historico_clinico__gestacoes__eventos_puerperio") }}
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
    ),

-- Dados de atendimento sao consolidados aqui para que os consumidores da camada canonica
-- nao precisem reconstruir os mesmos vinculos com cada prontuario.
    prontuario_alta_deduplicada AS (
        SELECT *
        FROM {{ ref("raw_prontuario_prontuaRio__internacao_alta") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(gid_prontuario AS STRING)
            ORDER BY loaded_at DESC
        ) = 1
    ),

    prontuario_cadastro_deduplicado AS (
        SELECT *
        FROM {{ ref("raw_prontuario_prontuaRio__internacao_cadastro") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(gid_prontuario AS STRING)
            ORDER BY loaded_at DESC
        ) = 1
    ),

    mv_admissao_deduplicada AS (
        SELECT
            * EXCEPT(id_hci),
            {{ dbt_utils.generate_surrogate_key([
                "id_atendimento",
                "id_cnes"
            ]) }} AS id_hci
        FROM {{ ref("raw_prontuario_mv__admissao") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(id_atendimento AS STRING), CAST(id_cnes AS STRING)
            ORDER BY updated_at DESC
        ) = 1
    ),

    mv_alta_deduplicada AS (
        SELECT *
        FROM {{ ref("raw_prontuario_mv__alta") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(id_hci AS STRING)
            ORDER BY updated_at DESC
        ) = 1
    ),

    mv_atendimento_deduplicado AS (
        SELECT *
        FROM {{ ref("raw_prontuario_mv__atendimento") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(id_hci AS STRING)
            ORDER BY updated_at DESC
        ) = 1
    ),

    mv_gestante_deduplicada AS (
        SELECT *
        FROM {{ ref("raw_prontuario_mv__gestante") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(id_hci AS STRING)
            ORDER BY updated_at DESC
        ) = 1
    ),

    vitai_boletim_deduplicado AS (
        SELECT *
        FROM {{ ref("raw_prontuario_vitai__boletim") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(gid AS STRING)
            ORDER BY updated_at DESC, imported_at DESC
        ) = 1
    ),

    vitai_estabelecimento_deduplicado AS (
        SELECT gid, cnes
        FROM {{ ref("raw_prontuario_vitai__m_estabelecimento") }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(gid AS STRING)
            ORDER BY updated_at DESC
        ) = 1
    ),

    sisare_gestante_deduplicada AS (
        SELECT *
        FROM {{ ref("int_subpav__sisare_gestantes") }}
        WHERE id_internacao IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(id_internacao AS STRING)
            ORDER BY datalake_loaded_at DESC, updated_at DESC
        ) = 1
    ),

    sisare_internacao_deduplicada AS (
        SELECT *
        FROM {{ ref("raw_plataforma_subpav_sisare__internacoes") }}
        WHERE id_internacao IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY CAST(id_internacao AS STRING)
            ORDER BY datalake_loaded_at DESC
        ) = 1
    ),

    eventos_com_atendimento AS (
        SELECT
            e.*,
            pc.paciente_cpf AS prontuario_cpf,
            pa.alta_data AS prontuario_data_alta,
            pa.cnes AS prontuario_cnes,
            ma.paciente_cpf AS mv_admissao_cpf,
            ma.id_cnes AS mv_admissao_cnes,
            mh.paciente_cpf AS mv_alta_cpf,
            mh.id_cnes AS mv_alta_cnes,
            mh.alta_medica_datahora AS mv_alta_medica_datahora,
            mh.alta_datahora_fechamento AS mv_alta_datahora_fechamento,
            mt.paciente_cpf AS mv_atendimento_cpf,
            mt.id_cnes AS mv_atendimento_cnes,
            mt.alta_datahora AS mv_atendimento_data_alta,
            mg.paciente_cpf AS mv_gestante_cpf,
            COALESCE(mg.id_cnes, mg.cnes) AS mv_gestante_cnes,
            mg.alta_medica_datahora AS mv_gestante_data_alta,
            vb.cpf AS vitai_boletim_cpf,
            vb.alta_data AS vitai_data_alta,
            ve.cnes AS vitai_cnes,
            sg.cpf AS sisare_cpf,
            sg.id_desfecho_gestacao AS sisare_id_desfecho_gestacao,
            sg.desfecho_gestacao AS sisare_desfecho_gestacao,
            si.dt_saida AS sisare_data_alta,
            si.unidade_atendimento AS sisare_cnes
        FROM eventos_deduplicados e
        LEFT JOIN prontuario_alta_deduplicada pa
            ON e.fonte = 'prontuaRio'
            AND CAST(pa.gid_prontuario AS STRING) = e.id_hci
        LEFT JOIN prontuario_cadastro_deduplicado pc
            ON e.fonte = 'prontuaRio'
            AND CAST(pc.gid_prontuario AS STRING) = e.id_hci
        LEFT JOIN mv_admissao_deduplicada ma
            ON e.fonte = 'mv'
            AND CAST(ma.id_hci AS STRING) = e.id_hci
        LEFT JOIN mv_alta_deduplicada mh
            ON e.fonte = 'mv'
            AND CAST(mh.id_hci AS STRING) = e.id_hci
        LEFT JOIN mv_atendimento_deduplicado mt
            ON e.fonte = 'mv'
            AND CAST(mt.id_hci AS STRING) = e.id_hci
        LEFT JOIN mv_gestante_deduplicada mg
            ON e.fonte = 'mv'
            AND CAST(mg.id_hci AS STRING) = e.id_hci
        LEFT JOIN vitai_boletim_deduplicado vb
            ON e.fonte = 'vitai'
            AND CAST(vb.gid AS STRING) = e.id_hci
        LEFT JOIN vitai_estabelecimento_deduplicado ve
            ON e.fonte = 'vitai'
            AND CAST(ve.gid AS STRING) = CAST(vb.gid_estabelecimento AS STRING)
        LEFT JOIN sisare_gestante_deduplicada sg
            ON e.fonte = 'sisare'
            AND CAST(sg.id_internacao AS STRING) = e.id_hci
        LEFT JOIN sisare_internacao_deduplicada si
            ON e.fonte = 'sisare'
            AND CAST(si.id_internacao AS STRING) = e.id_hci
    ),

    eventos_enriquecidos AS (
        SELECT
            e.id_evento_obstetrico,
            e.id_paciente,
            COALESCE(
                NULLIF(REGEXP_REPLACE(CAST(e.cpf AS STRING), r'\D', ''), ''),
                NULLIF(REGEXP_REPLACE(CAST(e.prontuario_cpf AS STRING), r'\D', ''), ''),
                NULLIF(REGEXP_REPLACE(CAST(e.mv_admissao_cpf AS STRING), r'\D', ''), ''),
                NULLIF(REGEXP_REPLACE(CAST(e.mv_alta_cpf AS STRING), r'\D', ''), ''),
                NULLIF(REGEXP_REPLACE(CAST(e.mv_atendimento_cpf AS STRING), r'\D', ''), ''),
                NULLIF(REGEXP_REPLACE(CAST(e.mv_gestante_cpf AS STRING), r'\D', ''), ''),
                NULLIF(REGEXP_REPLACE(CAST(e.vitai_boletim_cpf AS STRING), r'\D', ''), ''),
                NULLIF(REGEXP_REPLACE(CAST(e.sisare_cpf AS STRING), r'\D', ''), '')
            ) AS cpf,
            e.id_hci,
            e.fonte,
            e.id_evento_origem,
            e.data_evento,
            e.tipo_evento,
            e.subtipo_evento,
            e.data_inicio_gestacao,
            e.data_fim_gestacao,
            e.data_parto,
            e.data_puerperio,
            e.dpp,
            e.idade_gestacional_dias,
            e.cid,
            e.procedimento_codigo,
            e.procedimento_descricao,
            CASE
                WHEN e.fonte = 'prontuaRio'
                  AND (
                      e.data_parto IS NULL
                      OR DATE(e.prontuario_data_alta) >= e.data_parto
                  )
                    THEN DATE(e.prontuario_data_alta)
                WHEN e.fonte = 'vitai'
                  AND (
                      e.data_parto IS NULL
                      OR DATE(e.vitai_data_alta) >= e.data_parto
                  )
                    THEN DATE(e.vitai_data_alta)
                WHEN e.fonte = 'sisare'
                  AND (
                      e.data_parto IS NULL
                      OR DATE(e.sisare_data_alta) >= e.data_parto
                  )
                    THEN DATE(e.sisare_data_alta)
                WHEN e.fonte = 'mv' THEN
                    CASE
                        WHEN e.data_parto IS NULL
                          OR DATE(COALESCE(
                                e.mv_alta_medica_datahora,
                                e.mv_atendimento_data_alta,
                                e.mv_gestante_data_alta,
                                e.mv_alta_datahora_fechamento
                            )) >= e.data_parto
                            THEN DATE(COALESCE(
                                e.mv_alta_medica_datahora,
                                e.mv_atendimento_data_alta,
                                e.mv_gestante_data_alta,
                                e.mv_alta_datahora_fechamento
                            ))
                    END
            END AS data_alta_internacao,
            NULLIF(REGEXP_REPLACE(CAST(
                CASE
                    WHEN e.fonte = 'prontuaRio' THEN e.prontuario_cnes
                    WHEN e.fonte = 'vitai' THEN e.vitai_cnes
                    WHEN e.fonte = 'sisare' THEN CAST(e.sisare_cnes AS STRING)
                    WHEN e.fonte = 'mv' THEN COALESCE(
                        e.mv_admissao_cnes,
                        e.mv_alta_cnes,
                        e.mv_atendimento_cnes,
                        e.mv_gestante_cnes
                    )
                END AS STRING
            ), r'\D', ''), '') AS cnes_estabelecimento,
            CASE
                WHEN e.fonte = 'sisare' THEN e.sisare_id_desfecho_gestacao
            END AS id_desfecho_gestacao,
            CASE
                WHEN e.fonte = 'sisare' THEN e.sisare_desfecho_gestacao
            END AS desfecho_gestacao,
            e.loaded_at,
            e.data_particao
        FROM eventos_com_atendimento e
    )

SELECT
    *
FROM eventos_enriquecidos
