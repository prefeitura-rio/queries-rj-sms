{{
    config(
        alias="atendimento",
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
        select * except(created_at),created_at
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__atendimento_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    old_events_from_window as (
        select *, cast(null as string) as created_at
        from {{ source("brutos_prontuario_vitai_staging", "atendimento_eventos") }}
        {% if is_incremental() %}
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    events_from_window as (
        select * from old_events_from_window
        union all
        select * from new_events_from_window
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
    safe_cast(paciente_id as string) as gid_paciente,
    safe_cast(boletim_id as string) as gid_boletim,
    safe_cast(estabelecimento_id as string) as gid_estabelecimento,
    safe_cast(profissional_id as string) as gid_profissional,

    -- Campos
    safe_cast(tipo_atendimento as string) as atendimento_tipo,
    safe_cast(especialidade as string) as especialidade,
    timestamp_add(datetime(timestamp({{process_null('dthr_inicio')}}), 'America/Sao_Paulo'),interval 3 hour) as inicio_datahora,
    timestamp_add(datetime(timestamp({{process_null('dthr_fim')}}), 'America/Sao_Paulo'),interval 3 hour) as fim_datahora,
    safe_cast(cid_codigo as string) as cid_codigo,
    safe_cast(cid as string) as cid_nome,
    timestamp_add(datetime(timestamp({{process_null('datahora')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    safe_cast(codigo as string) as codigo,
    safe_cast(queixa as string) as queixa,
    safe_cast(cliente as string) as cliente,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
from latest_events
