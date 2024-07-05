{{
    config(
        alias="boletim",
        materialized="incremental",
        unique_key="id",
        tags=["vitai_db", "every_30_min"],
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    -- Seleciona eventos dos últimos 7 dias se for uma execução incremental
    events_from_window as (
        select *
        from {{ source("brutos_prontuario_vitai_staging", "boletim_eventos") }}
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
    safe_cast(gid as string) as id,

    -- Chaves Estrangeiras
    safe_cast(estabelecimento_gid as string) as id_estabelecimento,
    safe_cast(secao_gid as string) as id_secao,
    safe_cast(paciente_gid as string) as id_paciente,
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
    safe_cast(data_entrada as string) as data_entrada,
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
    safe_cast(data_internacao as string) as internacao_data,
    safe_cast(data_alta as string) as alta_data,
    safe_cast(cliente as string) as cliente,
    safe_cast(sigla as string) as sigla,
    safe_cast(nomeestabelecimento as string) as estabelecimento_nome,
    safe_cast(cpf as string) as cpf,
    safe_cast(cns as string) as cns,
    safe_cast(baseurl as string) as base_url,
    safe_cast(datahora as timestamp) as updated_at,
    safe_cast(datalake__imported_at as timestamp) as imported_at
from latest_events
