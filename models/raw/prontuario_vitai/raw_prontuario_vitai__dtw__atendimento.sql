{{
    config(
        alias="dtw__atendimento",
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
        from {{ source("brutos_prontuario_vitai_staging", "dtw__fat_atendimento_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    -- Ranqueia os eventos por frescor dentro de cada grupo
    events_ranked_by_freshness as (
        select *, 
            row_number() over (partition by atendimento_gid order by datalake_loaded_at desc) as rank
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
    SAFE_CAST(atendimento_gid AS STRING) AS gid,

    -- Chaves Estrangeiras
    SAFE_CAST(estabelecimento_gid AS STRING) AS gid_estabelecimento,
    SAFE_CAST(boletim_gid AS STRING) AS gid_boletim,
    SAFE_CAST(matd_id AS INTEGER) AS id_matd,
    SAFE_CAST(cid_id AS INTEGER) AS id_cid,
    SAFE_CAST(esp_id AS INTEGER) AS id_especialidade,
    SAFE_CAST(fat_paciente_rede_id AS INTEGER) AS id_paciente_rede,
    SAFE_CAST(prf_id AS INTEGER) AS id_prf,

    -- Campos de Texto e Descrição
    SAFE_CAST(queixa AS STRING) AS queixa_principal,
    SAFE_CAST(primeiro_atendimento AS STRING) AS primeiro_atendimento,
    SAFE_CAST(ultimo_atendimento AS STRING) AS ultimo_atendimento,

    -- Campos de Data e Horário
    SAFE_CAST(data_inicio AS TIMESTAMP) AS atendimento_inicio,
    SAFE_CAST(data_fim AS TIMESTAMP) AS atendimento_fim,

    safe_cast(null as TIMESTAMP) as updated_at,
    safe_cast(null as TIMESTAMP) as centralized_at,
    SAFE_CAST(datalake_loaded_at AS TIMESTAMP) AS imported_at
    
from latest_events