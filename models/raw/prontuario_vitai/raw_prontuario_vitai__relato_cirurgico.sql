{{
    config(
        alias="relato_cirurgico",
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
        select *, cast(null as string) as created_at
        from {{ source("brutos_prontuario_vitai_staging", "relato_cirurgico_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    new_events_from_window as (
        select * except(created_at), created_at
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__relato_cirurgico_eventos") }}
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
    safe_cast(boletim_gid as string) as boletim_gid,
    safe_cast(paciente_gid as string) as paciente_gid,
    safe_cast(estabelecimento_gid as string) as estabelecimento_gid,

    -- Campos
    safe_cast(cir_data as string) as cir_data,
    safe_cast(nmespecialidade as string) as nome_especialidade,
    safe_cast(status as string) as status,
    timestamp_add(datetime(timestamp({{process_null('checkin_sala')}}), 'America/Sao_Paulo'),interval 3 hour) as checkin_sala,
    timestamp_add(datetime(timestamp({{process_null('cirurgia_inicio')}}), 'America/Sao_Paulo'),interval 3 hour) as cirurgia_inicio,
    timestamp_add(datetime(timestamp({{process_null('cirurgia_fim')}}), 'America/Sao_Paulo'),interval 3 hour) as cirurgia_fim,
    timestamp_add(datetime(timestamp({{process_null('checkout_sala')}}), 'America/Sao_Paulo'),interval 3 hour) as checkout_sala,
    safe_cast(tempo_sala as string) as tempo_sala,
    safe_cast(anestesia as string) as anestesia,
    safe_cast(contaminacao as string) as contaminacao,
    safe_cast(codigo_procedimento as string) as codigo_procedimento,
    safe_cast(descricao_procedimento as string) as descricao_procedimento,
    safe_cast(codigo_diagnostico as string) as codigo_diagnostico,
    safe_cast(descricao_diagnostico as string) as descricao_diagnostico,
    safe_cast(leito_paciente as string) as leito_paciente,
    safe_cast(equipe as string) as equipe,
    timestamp_add(datetime(timestamp({{process_null('cir_datafinalizado')}}), 'America/Sao_Paulo'),interval 3 hour) as cir_datafinalizado,
    safe_cast(cliente as string) as cliente,
    safe_cast(baseurl as string) as baseurl,
    safe_cast(estabelecimento_sigla as string) as estabelecimento_sigla,
    safe_cast(codigointerno as string) as codigo_interno,

    -- Metadados
    timestamp_add(datetime(timestamp(datahora), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
    
from latest_events
