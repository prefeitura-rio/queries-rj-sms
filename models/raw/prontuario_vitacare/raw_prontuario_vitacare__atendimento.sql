

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

    atendimentos_deduplicados as (
        select *
        from atendimentos
        qualify row_number() over (partition by id order by updated_at desc) = 1
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
