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
    -- VITAI
    -- ##################################################
    vitai_estoque as (
        select distinct
            cnes as unidade_cnes,
            'vitai' as fonte,
            'posicao' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitai_staging", "estoque_posicao") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
            union all
        select distinct
            cnes as unidade_cnes,
            'vitai' as fonte,
            'movimento' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitai_staging", "estoque_movimento") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    unidades_vitai as (
        select 
            area_programatica as unidade_ap,
            id_cnes as unidade_cnes,
            nome_limpo as unidade_nome
        from {{ref('dim_estabelecimento')}}
        where prontuario_versao = 'vitai'
    ),
    vitai_agrupado as (
        select 
            fonte,
            tipo,
            data_atualizacao,
            count(unidade_cnes) as quant_unidades_com_dado,
            array_agg(unidade_cnes) as unidades_com_dado,
        from vitai_estoque
        group by 1, 2, 3
    ),
    vitai_agrupado_com_unidades as (
        select 
            * except(unidades_com_dado),
            array(
                select as struct *
                from unidades_vitai where unidade_cnes not in unnest(unidades_com_dado)
            ) as unidades_sem_dado
        from vitai_agrupado
    ),

    -- ##################################################
    -- VITACARE
    -- ##################################################
    vitacare_estoque as (
        select distinct
            cnesUnidade as unidade_cnes,
            'vitacare' as fonte,
            'posicao' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_posicao") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
            union all
        select distinct
            cnesUnidade as unidade_cnes,
            'vitacare' as fonte,
            'movimento' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_movimento") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    unidades_vitacare as (
        select
            area_programatica as unidade_ap,
            id_cnes as unidade_cnes,
            nome_limpo as unidade_nome
        from {{ref('dim_estabelecimento')}}
        where prontuario_versao = 'vitacare'
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
        select * from vitai_agrupado_com_unidades
            union all
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