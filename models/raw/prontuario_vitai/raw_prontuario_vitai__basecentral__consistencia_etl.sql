{{
    config(
        alias="basecentral__consistencia_etl",
        materialized="incremental",
        unique_key="gid",
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with 
    source as (
        select *
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__consistencia_etl") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),

    tabela_padronizada as (
        select 
            safe_cast(gid as string) as gid,
            safe_cast(contexto as string) as contexto,
            safe_cast(ano as int) as ano,
            safe_cast(mes as int) as mes,
            safe_cast(dia as int) as dia,
            safe_cast(qtd_registros as int) as quantidade_registros,
            timestamp_add(datetime(timestamp({{process_null('min_datahora')}}), 'America/Sao_Paulo'),interval 3 hour) as min_datahora,
            timestamp_add(datetime(timestamp({{process_null('max_datahora')}}), 'America/Sao_Paulo'),interval 3 hour) as max_datahora,
            safe_cast(estabelecimento_gid as string) as estabelecimento_gid,
            timestamp_add(datetime(timestamp({{process_null('datahora')}}), 'America/Sao_Paulo'),interval 3 hour) as datahora,
            timestamp_add(datetime(timestamp({{process_null('created_at')}}), 'America/Sao_Paulo'),interval 3 hour) as created_at,
            timestamp_add(datetime(timestamp({{process_null('updated_at')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
            datetime(timestamp(datalake_loaded_at), 'America/Sao_Paulo') as datalake_loaded_at,
            safe_cast(partition_date as date) as partition_date,
            safe_cast(ano_particao as int) as ano_particao,
            safe_cast(mes_particao as int) as mes_particao,
            safe_cast(data_particao as date) as data_particao
        from source
    )

select * 
from tabela_padronizada