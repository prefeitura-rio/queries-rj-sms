{{
    config(
        alias="atendimento",
        materialized="incremental",
        incremental_strategy='merge', 
        unique_key=['id_hci'],
        cluster_by="cpf",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        tags=['daily']
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

with
    atendimento_historico as (
        select *, 'historico' as origem, 1  as rank
        from {{ ref("base_prontuario_vitacare__atendimento_historico") }}
        {% if is_incremental() %}
        where data_particao >= {{ partitions_to_replace }}
        {% endif %}    
    ),
    atendimento_continuo as (
        select *, 'continuo' as origem, 2  as rank
        from {{ ref("base_prontuario_vitacare__atendimento_continuo") }}
        {% if is_incremental() %}
        where data_particao >= {{ partitions_to_replace }}
        {% endif %}
    ),

    atendimentos as (
        select * from atendimento_historico
        union all
        select * from atendimento_continuo
    ),
    atendimentos_teste_duplicados as (
        select id_hci, count(*) as qtd
        from atendimentos
        where cnes_unidade in ('6922031', '9391983', '7856954', '7414226') 
        and datahora_inicio > '2024-11-06'
        group by 1
        having count(*) > 1
    ),
    atendimentos_unicos as (
        select *
        from atendimentos
        where id_hci not in (select id_hci from atendimentos_teste_duplicados)
        qualify row_number() over (partition by id_prontuario_global order by rank, updated_at desc) = 1
    )

select 
    id_prontuario_local,
    id_prontuario_global,
    id_hci,
    cpf,
    cnes_unidade,
    cns_profissional,
    cpf_profissional,
    nome_profissional,
    cbo_profissional,
    cbo_descricao_profissional,
    cod_equipe_profissional,
    cod_ine_equipe_profissional,
    nome_equipe_profissional,
    tipo,
    eh_coleta,
    timestamp_add(datetime(timestamp(datahora_marcacao), 'America/Sao_Paulo'),interval 3 hour) as datahora_marcacao,
    timestamp_add(datetime(timestamp(datahora_inicio), 'America/Sao_Paulo'),interval 3 hour) as datahora_inicio,
    timestamp_add(datetime(timestamp(datahora_fim), 'America/Sao_Paulo'),interval 3 hour) as datahora_fim,
    soap_subjetivo_motivo,
    soap_objetivo_descricao,
    soap_avaliacao_observacoes,
    soap_plano_procedimentos_clinicos,
    soap_plano_observacoes,
    soap_notas_observacoes,
    prescricoes,
    condicoes,
    exames_solicitados,
    alergias_anamnese,
    vacinas,
    indicadores,
    encaminhamentos,
    timestamp_add(datetime(timestamp(updated_at), 'America/Sao_Paulo'),interval 3 hour) as updated_at,
    timestamp_add(datetime(timestamp(loaded_at), 'America/Sao_Paulo'),interval 3 hour) as loaded_at,
    data_particao,
    origem
from atendimentos_unicos
{% if is_incremental() %}
    where data_particao >= {{ partitions_to_replace }}
{% endif %}
