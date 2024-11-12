{{
    config(
        alias="basecentral__prescricao",
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
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__prescricao_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    
    -- Ranqueia os eventos por frescor dentro de cada grupo
    events_ranked_by_freshness as (
        select *, 
            row_number() over (partition by prescricao_gid order by datahora desc) as rank
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
    safe_cast(prescricao_gid as string) as gid,

    -- Chaves Estrangeiras
    safe_cast({{ process_null('paciente_gid') }} as string) as gid_paciente,
    safe_cast({{ process_null('boletim_gid') }} as string) as gid_boletim,
    safe_cast({{ process_null('estabelecimento_gid') }} as string) as gid_estabelecimento,
    safe_cast({{ process_null('profissional_gid') }} as string) as gid_profissional,

    -- Dados de Prescrição e Atendimento
    safe_cast(data_prescricao as timestamp) as data_prescricao,
    safe_cast({{ process_null('tipo') }} as string) as tipo_prescricao,
    safe_cast({{ process_null('pre_urgencia') }} as string) as urgencia,
    safe_cast({{ process_null('pre_rotinaenfermagem') }} as string) as rotina_enfermagem,

    -- Dados do Estabelecimento
    safe_cast({{ process_null('sigla') }} as string) as sigla_estabelecimento,
    safe_cast({{ process_null('nomeestabelecimento') }} as string) as nome_estabelecimento,
    safe_cast({{ process_null('baseurl') }} as string) as url_estabelecimento,

    -- Timestamps
    safe_cast(datahora as timestamp) as updated_at,
    safe_cast(created_at as timestamp) as centralized_at,
    safe_cast(datalake_loaded_at as timestamp) as imported_at,

    safe_cast(data_particao as date) as data_particao

from latest_events