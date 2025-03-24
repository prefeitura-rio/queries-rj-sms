{{
    config(
        alias="profissional",
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
        from {{ source("brutos_prontuario_vitai_staging", "profissional_eventos") }}
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
    safe_cast(estabelecimento_gid as string) as gid_estabelecimento,

    -- Campos
    safe_cast(numero_conselho as string) as numero_conselho,
    safe_cast(conselho_regional as string) as conselho_regional,
    safe_cast(codigo_interno as string) as codigo_interno,
    safe_cast(cpf as string) as cpf,
    safe_cast(cns as string) as cns,
    safe_cast(sigla_conselho as string) as sigla_conselho,
    safe_cast(nome as string) as nome,
    safe_cast(uf_conselho as string) as uf_conselho,
    safe_cast(cbo_descricao as string) as cbo_descricao,
    safe_cast(cbo as string) as cbo,
    safe_cast(situacao as string) as situacao,
    safe_cast(cliente as string) as cliente,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
    
from latest_events
