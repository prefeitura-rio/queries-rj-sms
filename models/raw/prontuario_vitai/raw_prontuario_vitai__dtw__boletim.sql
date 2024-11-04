{{
    config(
        alias="dtw__boletim",
        materialized="incremental",
        unique_key="gid",
        tags=["every_30_min"],
        enabled=false
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    -- Seleciona eventos dos últimos 7 dias se for uma execução incremental
    events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitai_staging", "dtw__fat_boletim_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    -- Ranqueia os eventos por frescor dentro de cada grupo
    events_ranked_by_freshness as (
        select *, 
            row_number() over (partition by boletim_gid order by datahora desc) as rank
        from events_from_window
    ),
    
    -- Seleciona apenas os eventos mais recentes de cada grupo
    latest_events as (
        select * 
        from events_ranked_by_freshness 
        where rank = 1
    )
   
-- Seleciona e converte os campos para o tipo apropriado
select
    -- Chave Primária
    SAFE_CAST(boletim_gid AS STRING) AS gid,

    -- Chaves Estrangeiras
    SAFE_CAST(estabelecimento_gid AS STRING) AS gid_estabelecimento,
    SAFE_CAST(fat_paciente_rede_id AS INTEGER) AS id_fat_paciente_rede,
    SAFE_CAST(tpa_id AS INTEGER) AS id_tpa,
    SAFE_CAST(tpu_id AS INTEGER) AS id_tpu,
    SAFE_CAST(tpe_id AS INTEGER) AS id_tpe,
    SAFE_CAST(esp_id AS INTEGER) AS id_especialidade,
    SAFE_CAST(ris_id AS INTEGER) AS id_ris,
    SAFE_CAST(cbo_id AS INTEGER) AS id_cbo,

    SAFE_CAST(interno AS STRING) AS paciente_interno,
    SAFE_CAST(numero_be AS STRING) AS boletim_numero,
    SAFE_CAST(data_entrada AS TIMESTAMP) AS data_entrada,
    SAFE_CAST(data_internacao AS TIMESTAMP) AS data_internacao,
    SAFE_CAST(data_alta AS TIMESTAMP) AS data_alta,
    SAFE_CAST(datahora AS TIMESTAMP) AS data_hora_evento,

    safe_cast(datahora as TIMESTAMP) as updated_at,
    safe_cast(null as TIMESTAMP) as centralized_at,
    SAFE_CAST(datalake_loaded_at AS TIMESTAMP) AS imported_at
    
from latest_events