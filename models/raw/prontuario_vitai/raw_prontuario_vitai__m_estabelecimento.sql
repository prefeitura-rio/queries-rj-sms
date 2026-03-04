{{
    config(
        alias="estabelecimento",
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
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__m_estabelecimento_eventos") }}
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
    safe_cast(gid as string) as gid,
    safe_cast(cnes as string) as cnes,
    safe_cast(cnpj as string) as cnpj,
    safe_cast(sigla as string) as sigla,
    safe_cast(nomeestabelecimento as string) as nome_estabelecimento,
    safe_cast(idsetor as string) as idsetor,
    safe_cast(secretaria_gid as string) as secretaria_gid,
    safe_cast(cliente as string) as cliente,
    safe_cast(baseurl as string) as baseurl,
    timestamp_add(datetime(timestamp(datahora), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
    
from latest_events
