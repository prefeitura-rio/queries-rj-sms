{{
    config(
        alias='serie_temporal_atendimentos_unidade_pcsm',
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "data_registro",
            "data_type": "date",
            "granularity": "day"
        },
        unique_key=['cnes', 'data_registro'],
        description='Série temporal de atendimentos por data de entrada no prontuário do PCSM, segmentada por unidade de saúde',
        tags=['datalake']
    )
}}

{% set last_partition = get_last_partition_date(this) %}

with
    atendimentos as (
        select
            id_atendimento,
            u.codigo_nacional_estabelecimento_saude as cnes,  
            data_entrada_atendimento as data_registro
        from {{ref('raw_pcsm_atendimentos')}} a 
        left join {{ref('raw_pcsm_unidades_saude')}} u 
            on u.id_unidade_saude = a.id_unidade_saude
        {% if is_incremental() %}
            where data_entrada_atendimento >= date('{{ last_partition }}')
        {% endif %}
    ),

    grouped_by_cnes_and_date as (
        select
            cnes,
            data_registro,
            count(id_atendimento) as atendimentos
        from atendimentos
        group by 1,2
    ),

    final as (
        select
            cnes,
            {{proper_estabelecimento('nome_acentuado')}} as nome,
            data_registro, 
            atendimentos
        from grouped_by_cnes_and_date g
        inner join {{ref('dim_estabelecimento')}} e
            on g.cnes = e.id_cnes
        where data_registro is not null
    )

select * from final