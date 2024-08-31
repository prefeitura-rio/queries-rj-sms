{{
    config(
        alias="resumo_alta",
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
        from {{ source("brutos_prontuario_vitai_staging", "resumo_alta_eventos") }}
        {% if is_incremental() %} where data_particao > '{{seven_days_ago}}' {% endif %}
    ),
    -- Ranqueia os eventos por frescor dentro de cada grupo
    events_ranked_by_freshness as (
        select *, 
            row_number() over (partition by resumo_gid order by datahora desc) as rank
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
    safe_cast(resumo_gid as string) as gid,

    -- Chaves Estrangeiras
    safe_cast(boletim_gid as string) as gid_boletim,
    safe_cast(paciente_gid as string) as gid_paciente,
    safe_cast(estabelecimentogid as string) as gid_estabelecimento,
    safe_cast(alta_medica_id as string) as id_alta_medica,

    -- Campos
    
    safe_cast(datahora as timestamp) as updated_at,
    safe_cast(tipo_alta as string) as alta_tipo,
    safe_cast(data_resumo_alta as timestamp) as resumo_alta_datahora,
    safe_cast(recomendacoes as string) as recomendacao,
    safe_cast(resumo_alta as string) as resumo_alta_descricao,
    safe_cast(conduta_alta as string) as alta_conduta,
    safe_cast(exames_alta as string) as alta_exames,
    safe_cast(tratamento_alta as string) as alta_tratamento,
    safe_cast(unr_id as string) as id_unr,
    safe_cast(unr_nome as string) as unr_nome,
    safe_cast(cid_codigo_alta as string) as cid_codigo_alta,
    safe_cast(cid_descricao_alta as string) as cid_descricao_alta,
    safe_cast(desfechointernacao as string) as desfecho_internacao,
    safe_cast(descricao_servico as string) as servico_descricao,
    safe_cast(data_exclusao_alta as timestamp) as exclusao_alta_datahora,
    safe_cast(motivo_exclusao as string) as exclusao_motivo,
    safe_cast(profissional_alta as string) as profissional_alta,

    -- Metadados
    safe_cast(cliente as string) as cliente,
    safe_cast(baseurl as string) as baseurl,
    safe_cast(estabelecimento_sigla as string) as estabelecimento_sigla,
    safe_cast(datalake__imported_at as timestamp) as imported_at,
    safe_cast(data_particao as date) as data_particao
    
from latest_events