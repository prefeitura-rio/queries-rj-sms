{{
    config(
        alias="cirurgia",
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
        from {{ source("brutos_prontuario_vitai_staging", "cirurgia_eventos") }}
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
    safe_cast(paciente_gid as string) as gid_paciente,
    safe_cast(boletim_gid as string) as gid_boletim,
    safe_cast(estabelecimento_gid as string) as gid_estabelecimento,

    -- Campos
    safe_cast(estabelecimento_sigla as string) as estabelecimento_sigla,
    safe_cast(data_cirurgia as string) as cirurgia_data,
    safe_cast(status as string) as status,
    safe_cast(procedimento_codigo as string) as procedimento_codigo,
    safe_cast(procedimento_nome as string) as procedimento_nome,
    safe_cast(diagnostico_codigo as string) as diagnostico_codigo,
    safe_cast(diagnostico_descricao as string) as diagnostico_descricao,
    safe_cast(especialidade as string) as especialidade,
    safe_cast(baseurl as string) as base_url,
    safe_cast(cliente as string) as cliente,
    safe_cast(datahora as timestamp) as updated_at,
    safe_cast(datalake__imported_at as timestamp) as imported_at,
    safe_cast(data_particao as date) as data_particao
from latest_events
