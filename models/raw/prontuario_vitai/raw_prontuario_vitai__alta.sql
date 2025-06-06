{{
    config(
        alias="alta",
        materialized="incremental",
        unique_key="gid",
        tags=["every_30_min"],
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    -- Seleciona eventos dos últimos 7 dias se for uma execução incremental
    new_events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__alta_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    old_events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitai_staging", "alta_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    events_from_window as (
        select *
        from old_events_from_window
        union all 
        select *
        from new_events_from_window
    ),
    
    -- Ranqueia os eventos por frescor dentro de cada grupo
    events_ranked_by_freshness as (
        select *, 
            row_number() over (partition by gid order by datahora desc) as rank
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
    safe_cast(gid as string) as gid,

    -- Chaves Estrangeiras
    safe_cast(boletim_gid as string) as gid_boletim,
    safe_cast(estabelecimento_gid as string) as gid_estabelecimento,

    -- Campos
    safe_cast(tipo_alta_detalhada as string) as alta_tipo_detalhado,
    safe_cast(tipo_alta as string) as alta_tipo,
    safe_cast(data_obito as string) as obito_data,
    safe_cast(data_alta as string) as alta_data,
    safe_cast(motivo_saida as string) as saida_motivo,
    safe_cast(alta_medica as string) as alta_medica,
    safe_cast(alta_administrativa as timestamp) as alta_administrativa_data,
    safe_cast(abe_obs_ as string) as abe_obs,
    safe_cast(status as string) as alta_status,
    timestamp_add(datetime(timestamp(datahora), 'America/Sao_Paulo'),interval 3 hour) as datahora,
    safe_cast(baseurl as string) as base_url,
    safe_cast(cliente as string) as cliente,
    timestamp_add(datetime(timestamp(created_at), 'America/Sao_Paulo'),interval 3 hour) as created_at,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
from latest_events
