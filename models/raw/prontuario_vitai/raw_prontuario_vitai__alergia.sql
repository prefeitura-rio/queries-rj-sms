{{
    config(
        alias="alergia",
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
    old_events_from_window as (
        select * except(datalake__imported_at), cast(null as string) as created_at,datalake__imported_at
        from {{ source("brutos_prontuario_vitai_staging", "alergia_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    new_events_from_window as (
        select * except(created_at,datalake_loaded_at), created_at, datalake_loaded_at as datalake__imported_at
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__alergia_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    events_from_window as (
        select * from new_events_from_window
        union all
        select * from old_events_from_window
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
    safe_cast(boletim_id as string) as gid_boletim,
    safe_cast(paciente_id as string) as gid_paciente,

    -- Campos
    safe_cast(severidade as string) as severidade,
    timestamp_add(datetime(timestamp(datahora), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    safe_cast(tipo as string) as tipo,
    safe_cast(descricao as string) as descricao,
    safe_cast(alg_ativo as string) as alg_ativo,
    safe_cast(data_registro as string) as data_registro,
    safe_cast(reacao as string) as reacao,
    safe_cast(inicio_sintomas as string) as inicio_sintomas,
    safe_cast(observacao as string) as observacao,
    safe_cast(cpf as string) as cpf,
    safe_cast(cns as string) as cns,
    safe_cast(cliente as string) as cliente,
    safe_cast(sigla as string) as sigla,
    safe_cast(nomeestabelecimento as string) as estabelecimento_nome,
    safe_cast(baseurl as string) as base_url,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
from latest_events
