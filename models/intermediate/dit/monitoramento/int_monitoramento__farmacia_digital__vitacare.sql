{{
    config(
        schema="gerenciamento__monitoramento",
        alias="estatisticas_farmacia_digital_vitacare",
        materialized="incremental",
        unique_key="id",
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

with
    unidades_esperadas as (
        select 
            area_programatica as unidade_ap,
            id_cnes as unidade_cnes,
            nome_limpo as unidade_nome
        from {{ref('dim_estabelecimento')}}
        where prontuario_estoque_tem_dado = 'sim' and prontuario_versao = 'vitacare' 
    ),
    vitacare_estoque_posicao as (
        select
            cnesUnidade as unidade_cnes,
            'vitacare' as fonte,
            'posicao' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_posicao") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    vitacare_estoque_movimento as (
        select
            cnesUnidade as unidade_cnes,
            'vitacare' as fonte,
            'movimento' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_movimento") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    vitacare_estoque as (
        select * from vitacare_estoque_posicao
        union all
        select * from vitacare_estoque_movimento
    ),
    contagem as (
        select 
            data_atualizacao,
            fonte,
            tipo,
            array_agg(distinct unidade_cnes) as unidades_com_dado,
            count(*) as qtd_registros_recebidos
        from vitacare_estoque
        group by 1, 2, 3
    ),
    contagem_com_complemento as (
        select 
            * except(unidades_com_dado),
            array(
                select as struct *
                from unidades_esperadas where unidade_cnes in unnest(unidades_com_dado)
            ) as unidades_com_dado,
            array(
                select as struct *
                from unidades_esperadas where unidade_cnes not in unnest(unidades_com_dado)
            ) as unidades_sem_dado
        from contagem
    )
select
    concat(data_atualizacao, '.', fonte, '.', tipo) as id,
    *
from contagem_com_complemento
order by id