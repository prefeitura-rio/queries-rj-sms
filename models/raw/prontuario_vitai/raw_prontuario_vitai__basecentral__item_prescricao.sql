{{
    config(
        alias="basecentral__item_prescricao",
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
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__item_prescricao_eventos") }}
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
    -- Identificadores Principais
    safe_cast(gid as string) as gid,
    safe_cast({{ process_null('estabelecimento_gid') }} as string) as gid_estabelecimento,
    safe_cast({{ process_null('boletim_gid') }} as string) as gid_boletim,
    safe_cast({{ process_null('prescricao_gid') }} as string) as gid_prescricao,

    -- Detalhes do Item Prescrito
    safe_cast({{ process_null('item_prescrito') }} as string) as item_prescrito,
    safe_cast({{ process_null('pri_descricaoitem') }} as string) as descricao_item,
    safe_cast(cast(quantidade as float64) as integer) as quantidade,
    safe_cast({{ process_null('unidade_medida') }} as string) as unidade_medida,
    safe_cast({{ process_null('via_administracao') }} as string) as via_administracao,
    safe_cast({{ process_null('observacao') }} as string) as observacao,
    safe_cast({{ process_null('orientacao_uso') }} as string) as orientacao_uso,
    safe_cast({{ process_null('is_antibiotico') }} as string) as is_antibiotico,

    -- Produto Associado
    safe_cast({{ process_null('produto_associado') }} as string) as produto_associado,
    safe_cast({{ process_null('produto_gid') }} as string) as gid_produto,
    safe_cast({{ process_null('tipo') }} as string) as tipo_produto,


    -- Dados do Estabelecimento
    safe_cast({{ process_null('sigla') }} as string) as sigla_estabelecimento,
    safe_cast({{ process_null('nomeestabelecimento') }} as string) as nome_estabelecimento,
    safe_cast({{ process_null('baseurl') }} as string) as url_estabelecimento,

    -- Timestamps e Datas
    timestamp_add(datetime(timestamp(datahora), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    timestamp_add(datetime(timestamp(created_at), 'America/Sao_Paulo'),interval 3 hour) as centralized_at,
    datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as loaded_at,

    safe_cast(data_particao as date) as data_particao

from latest_events