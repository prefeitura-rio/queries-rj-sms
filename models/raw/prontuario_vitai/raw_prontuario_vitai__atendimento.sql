{{
    config(
        alias="atendimento",
        materialized="incremental",
        unique_key="id",
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
        from {{ source("brutos_prontuario_vitai_staging", "atendimento_eventos") }}
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
    safe_cast(tipo_atendimento as string) as atendimento_tipo,
    safe_cast(especialidade as string) as especialidade,
    safe_cast(dthr_inicio as string) as inicio_datahora,
    safe_cast(dthr_fim as string) as fim_datahora,
    safe_cast(cid_codigo as string) as cid_codigo,
    safe_cast(cid as string) as cid_nome,
    safe_cast(datahora as timestamp) as updated_at,
    safe_cast(codigo as string) as codigo,
    safe_cast(queixa as string) as queixa,
    safe_cast(cliente as string) as cliente,
    safe_cast(datalake__imported_at as timestamp) as imported_at
from latest_events
