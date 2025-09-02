{{
    config(
        schema="gerenciamento__monitoramento",
        alias="estatisticas_farmacia",
        materialized="incremental",
        unique_key="id",
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with

    -- ##################################################
    -- VITACARE
    -- ##################################################
    vitacare_estoque as (
        select distinct
            id_cnes as unidade_cnes,
            'vitacare' as fonte,
            'posicao' as tipo,
            particao_data_posicao as data_atualizacao
        from {{ ref("raw_prontuario_vitacare__estoque_posicao") }}
        {% if is_incremental() %} 
        where particao_data_posicao > '{{seven_days_ago}}' 
        {% endif %}
            union all
        select distinct
            id_cnes as unidade_cnes,
            'vitacare' as fonte,
            'movimento' as tipo,
            particao_data_movimento as data_atualizacao
        from {{ ref("raw_prontuario_vitacare__estoque_movimento") }}
        {% if is_incremental() %} 
        where particao_data_movimento > '{{seven_days_ago}}' 
        {% endif %}
    ),
    unidades_vitacare as (
        select
            area_programatica as unidade_ap,
            id_cnes as unidade_cnes,
            nome_limpo as unidade_nome
        from {{ref('dim_estabelecimento')}}
        where prontuario_versao = 'vitacare' and prontuario_estoque_tem_dado = 'sim'
    ),
    vitacare_agrupado as (
        select 
            fonte,
            tipo,
            data_atualizacao,
            count(unidade_cnes) as quant_unidades_com_dado,
            array_agg(unidade_cnes) as unidades_com_dado,
        from vitacare_estoque
        group by 1, 2, 3
    ),
    vitacare_agrupado_com_unidades as (
        select 
            * except(unidades_com_dado),
            array(
                select as struct *
                from unidades_vitacare where unidade_cnes not in unnest(unidades_com_dado)
            ) as unidades_sem_dado
        from vitacare_agrupado
    ),

    -- ##################################################
    -- JUNTANDO
    -- ##################################################
    unioned as (
        select * from vitacare_agrupado_com_unidades
    ),
    with_key as (
        select 
            concat(data_atualizacao, '.', fonte, '.', tipo) as id,
            *
        from unioned
    )
select
    *
from with_key
order by id