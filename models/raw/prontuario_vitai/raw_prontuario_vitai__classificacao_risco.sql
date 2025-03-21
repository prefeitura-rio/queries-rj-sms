{{
    config(
        alias="classificacao_risco",
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
    events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitai_staging", "classificacao_risco_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
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
    safe_cast(asv_queixa_principal as string) as asv_queixa_principal,
    safe_cast(risco as string) as risco,
    timestamp_add(datetime(timestamp({{process_null('dthr_inicio')}}), 'America/Sao_Paulo'),interval 3 hour) as inicio_datahora,
    timestamp_add(datetime(timestamp({{process_null('dthr_fim')}}), 'America/Sao_Paulo'),interval 3 hour) as fim_datahora,
    safe_cast(clr_meta as string) as clr_meta,
    safe_cast(descritor as string) as descritor,
    safe_cast(codigo as string) as codigo,
    safe_cast(cliente as string) as cliente,
    timestamp_add(datetime(timestamp(datahora), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
from latest_events
