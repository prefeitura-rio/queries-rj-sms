{{
    config(
        alias="dtw__internacao",
        materialized="incremental",
        unique_key="gid",
        tags=["every_30_min"],
        enabled=false,
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    -- Seleciona eventos dos últimos 7 dias se for uma execução incremental
    events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitai_staging", "dtw__fat_internacao_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    -- Ranqueia os eventos por frescor dentro de cada grupo
    events_ranked_by_freshness as (
        select *, 
            row_number() over (partition by internacao_gid order by datalake_loaded_at desc) as rank
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
    -- Chaves Primárias e Estrangeiras
    SAFE_CAST(internacao_gid AS STRING) AS gid,
    SAFE_CAST(estabelecimento_gid AS STRING) AS gid_estabelecimento,
    SAFE_CAST(boletim_gid AS STRING) AS gid_boletim,
    SAFE_CAST(fat_paciente_rede_id AS INTEGER) AS id_paciente_rede,

    -- Campos de Data e Horário
    SAFE_CAST(data_entrada AS TIMESTAMP) AS data_entrada,
    SAFE_CAST(data_saida AS TIMESTAMP) AS data_saida,

    -- Identificadores e Relacionamentos
    SAFE_CAST(tin_id AS INTEGER) AS id_tipo_internacao,
    SAFE_CAST(cid_id_internacao AS INTEGER) AS id_cid_internacao,
    SAFE_CAST(prc_id AS INTEGER) AS id_procedimento,
    SAFE_CAST(esp_id AS INTEGER) AS id_especialidade,
    SAFE_CAST(prf_id AS INTEGER) AS id_profissional,

    safe_cast(null as TIMESTAMP) as updated_at,
    safe_cast(null as TIMESTAMP) as centralized_at,
    SAFE_CAST(datalake_loaded_at AS TIMESTAMP) AS imported_at
    
from latest_events