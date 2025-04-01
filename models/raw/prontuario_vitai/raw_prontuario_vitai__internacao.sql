{{
    config(
        alias="internacao",
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
        select *, cast(null as string) as estabelecimento_sigla
        from {{ source("brutos_prontuario_vitai_staging", "internacao_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    new_events_from_window as (
        select * except(estabelecimento_sigla), estabelecimento_sigla
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__internacao_eventos") }}
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
    safe_cast(paciente_gid as string) as gid_paciente,
    safe_cast(boletim_gid as string) as gid_boletim,
    safe_cast(estabelecimento_gid as string) as gid_estabelecimento,
    safe_cast(profissional_gid as string) as gid_profissional,
    safe_cast(procedimento_codigo as string) as id_procedimento,
    safe_cast(diagnostico_codigo as string) as id_diagnostico,

    -- Campos
    safe_cast(tipo_internacao as string) as internacao_tipo,
    safe_cast(especialidade as string) as especialidade,
    safe_cast(cpf as string) as cpf,
    safe_cast(cns as string) as cns,
    safe_cast(procedimento_nome as string) as procedimento_nome,
    safe_cast(profissional_responsavel as string) as profissional_nome,
    safe_cast(diagnostico_descricao as string) as diagnostico_descricao,
    timestamp_add(datetime(timestamp({{process_null('data_saida')}}), 'America/Sao_Paulo'),interval 3 hour) as saida_data,
    timestamp_add(datetime(timestamp({{process_null('datainternacao')}}), 'America/Sao_Paulo'),interval 3 hour) as internacao_data,
    safe_cast(baseurl as string) as base_url,
    safe_cast(cliente as string) as cliente,
    timestamp_add(datetime(timestamp({{process_null('datahora')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    timestamp_add(datetime(timestamp({{process_null('created_at')}}), 'America/Sao_Paulo'),interval 3 hour) as created_at,
    safe_cast(data_particao as date) as data_particao
    
from latest_events
