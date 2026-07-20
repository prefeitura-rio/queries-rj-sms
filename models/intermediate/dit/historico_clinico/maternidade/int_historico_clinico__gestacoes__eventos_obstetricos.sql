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
    )

SELECT
    *
FROM eventos_deduplicados
