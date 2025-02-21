

{{
    config(
        alias="atendimento",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        cluster_by="cpf",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

with

    atendimentos as (
        select *, 'rotineiro' as origem,
        from {{ ref("base_prontuario_vitacare__atendimento_rotineiro") }}
        union all
        select *, 'historico' as origem
        from {{ ref("base_prontuario_vitacare__atendimento_historico") }}
    ),
    atendimentos_teste_duplicados as (
        select id_hci, count(*) as qtd
        from atendimentos
        where cnes_unidade in ('6922031', '9391983', '7856954', '7414226') 
        and datahora_inicio > '2024-11-06'
        group by 1
    ),
    atendimentos_sem_teste_duplicados as (
        select *
        from atendimentos
        where id_hci not in (select id_hci from atendimentos_teste_duplicados)
    ),

    atendimentos_deduplicados as (
        select *
        from atendimentos_sem_teste_duplicados
        qualify row_number() over (partition by id_prontuario_global order by updated_at desc) = 1
    ),

    atendimentos_unicos as (
        select * 
        from atendimentos_deduplicados
    )

select *
from atendimentos_unicos
{% if is_incremental() %}
    where data_particao >= {{ partitions_to_replace }}
{% endif %}
