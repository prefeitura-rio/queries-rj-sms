{{
    config(
        alias="exame",
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
        select *,cast(null as string) as created_at
        from {{ source("brutos_prontuario_vitai_staging", "exame_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),

    new_events_from_window as (
        select * except(created_at), created_at
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__exame_eventos") }}
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
    safe_cast(paciente_id as string) as gid_paciente,
    safe_cast(boletim_id as string) as gid_boletim,
    safe_cast(estabelecimento_id as string) as gid_estabelecimento,
    safe_cast(medico_solicitante_gid as string) as gid_medico_solicitante,
    safe_cast(exame_item_id as string) as id_exame_item,
    safe_cast(pedidoid as string) as id_pedido,

    -- Campos
    safe_cast(statuspedido as string) as status_pedido,
    timestamp_add(datetime(timestamp({{process_null('dataliberacao')}}), 'America/Sao_Paulo'),interval 3 hour) as liberacao_data,
    timestamp_add(datetime(timestamp({{process_null('datapedido')}}), 'America/Sao_Paulo'),interval 3 hour) as pedido_data,
    timestamp_add(datetime(timestamp({{process_null('datarealizacao')}}), 'America/Sao_Paulo'),interval 3 hour) as realizacao_data,
    safe_cast(indicacaoclinica as string) as indicacao_clinica,
    safe_cast(mnemonico as string) as mnemonico,
    safe_cast(tipo as string) as tipo,
    safe_cast(exame as string) as exame_descricao,
    safe_cast(medico_solicitante_nome as string) as medico_solicitante_nome,
    safe_cast(statusitem as string) as status_item,
    safe_cast(cpf as string) as cpf,
    safe_cast(cns as string) as cns,
    safe_cast(baseurl as string) as base_url,
    safe_cast(cliente as string) as cliente,
    safe_cast(sigla as string) as sigla,
    safe_cast(nomeestabelecimento as string) as estabelecimento_nome,
    safe_cast(procedimentocodigo as string) as procedimento_codigo,
    safe_cast(procedimentonome as string) as procedimento_nome,
    timestamp_add(datetime(timestamp(datahora), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
from latest_events
