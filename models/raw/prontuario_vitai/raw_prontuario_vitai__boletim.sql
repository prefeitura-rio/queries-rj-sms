{{
    config(
        alias="boletim",
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
        from {{ source("brutos_prontuario_vitai_staging", "boletim_eventos") }}
        {% if is_incremental() %} 
            where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    new_events_from_window as (
        select * except(created_at),created_at
        from {{ source("brutos_prontuario_vitai_staging", "basecentral__boletim_eventos") }}
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
    safe_cast(gid as string) as id_prontuario_global,
    {{
        dbt_utils.generate_surrogate_key(
                [
                    "gid",
                ]
            )
        }} as id_hci,
    -- Chaves Estrangeiras
    safe_cast(estabelecimento_gid as string) as gid_estabelecimento,
    safe_cast(secao_gid as string) as gid_secao,
    safe_cast(paciente_gid as string) as gid_paciente,
    safe_cast(setorid as string) as id_setor,
    safe_cast(idcidadao as string) as id_cidadao,
    safe_cast(sk_data as string) as sk_data,

    -- Campos
    safe_cast(tipo_atendimento as string) as atendimento_tipo,
    safe_cast(meiotransportenome as string) as meio_transporte_nome,
    safe_cast(clinicanome as string) as clinica_nome,
    safe_cast(motivoatendimentonome as string) as motivo_atendimento_nome,
    safe_cast(cancelado as string) as cancelado,
    safe_cast(plano_saude as string) as plano_saude,
    timestamp_add(datetime(timestamp({{process_null('data_entrada')}}), 'America/Sao_Paulo'),interval 3 hour) as data_entrada,
    safe_cast(motivoatendimentoid as string) as id_motivo_atendimento,
    safe_cast(sobcustodia as string) as sob_custodia,
    safe_cast(nomesecao as string) as secao_nome,
    safe_cast(detalheacidentenome as string) as detalhe_acidente_nome,
    safe_cast(tipo_unidade_entrada as string) as unidade_entrada_tipo,
    safe_cast(pulseira as string) as pulseira,
    safe_cast(especialidadenome as string) as especialidade_nome,
    safe_cast(convenio as string) as convenio,
    safe_cast(estabelecimento_sigla as string) as estabelecimento_sigla,
    safe_cast(numerobe as string) as numero_be,
    safe_cast(origemdescricao as string) as origem_descricao,
    safe_cast(tipo_entrada as string) as entrada_tipo,
    safe_cast(interno as string) as interno,
    safe_cast(cbo_descricao as string) as cbo_descricao,
    safe_cast(cbo_codigo as string) as cbo_codigo,
    timestamp_add(datetime(timestamp({{process_null('data_internacao')}}), 'America/Sao_Paulo'),interval 3 hour) as internacao_data,
    timestamp_add(datetime(timestamp({{process_null('data_alta')}}), 'America/Sao_Paulo'),interval 3 hour) as alta_data,
    safe_cast(cliente as string) as cliente,
    safe_cast(sigla as string) as sigla,
    safe_cast(nomeestabelecimento as string) as estabelecimento_nome,
    safe_cast(cpf as string) as cpf,
    safe_cast(cns as string) as cns,
    safe_cast(baseurl as string) as base_url,
    timestamp_add(datetime(timestamp({{process_null('datahora')}}), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    datetime(timestamp(datalake__imported_at), 'America/Sao_Paulo') as imported_at,
    safe_cast(data_particao as date) as data_particao
from latest_events
