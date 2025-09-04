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

{% set min_date = '2024-12-01' %}

with
    unidades_esperadas as (
        select 
            area_programatica as unidade_ap,
            id_cnes as unidade_cnes,
            nome_limpo as unidade_nome
        from {{ref('dim_estabelecimento')}}
        where 
            prontuario_tem = "sim" and 
            prontuario_estoque_tem_dado = 'sim' and 
            prontuario_versao = 'vitacare' 
    ),
    vitacare_estoque_posicao as (
        select
            id_cnes as unidade_cnes,
            'vitacare' as fonte,
            'posicao' as tipo,
            safe_cast(metadados.loaded_at as date) as data_ingestao
        from {{ ref('raw_prontuario_vitacare_api_centralizadora__estoque_posicao') }}
        {% if is_incremental() %} 
        where particao_data_posicao > '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %}
        where particao_data_posicao >= '{{min_date}}' 
        {% endif %}
    ),
    vitacare_estoque_movimento as (
        select
            id_cnes as unidade_cnes,
            'vitacare' as fonte,
            'movimento' as tipo,
            safe_cast(metadados.loaded_at as date) as data_ingestao
        from {{ ref('raw_prontuario_vitacare_api_centralizadora__estoque_movimento') }}
        {% if is_incremental() %} 
        where particao_data_movimento > '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %}
        where particao_data_movimento >= '{{min_date}}' 
        {% endif %}
    ),
    vitacare_vacina as (
        select
            id_cnes as unidade_cnes,
            'vitacare' as fonte,
            'vacina' as tipo,
            safe_cast(metadados.loaded_at as date) as data_ingestao
        from {{ ref('raw_prontuario_vitacare_api_centralizadora__vacinacao') }}
        {% if is_incremental() %} 
        where particao_data_vacinacao > '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %}
        where particao_data_vacinacao >= '{{min_date}}' 
        {% endif %}
    ),
    datas as (
        select data_ingestao 
        {% if is_incremental() %}
        from unnest(GENERATE_DATE_ARRAY('{{seven_days_ago}}', current_date())) as data_ingestao
        {% endif %}
        {% if not is_incremental() %}
        from unnest(GENERATE_DATE_ARRAY('{{min_date}}', current_date())) as data_ingestao
        {% endif %}
    ),
    entidades as (
        select tipo from unnest(['posicao','movimento','vacina']) tipo
    ),
    todos_registros_possiveis as (
        select 
            cast(null as string) as unidade_cnes,
            'vitacare' as fonte, 
            entidades.tipo as tipo, 
            cast(datas.data_ingestao as date) as data_ingestao,
        from datas, entidades
    ),
    vitacare_estoque as (
        select * from vitacare_estoque_posicao
        union all
        select * from vitacare_estoque_movimento
        union all
        select * from vitacare_vacina
        union all
        select * from todos_registros_possiveis
    ),
    contagem as (
        select 
            data_ingestao,
            fonte,
            tipo,
            array_agg(distinct unidade_cnes ignore nulls) as unidades_com_dado,
            countif(unidade_cnes is not null) as qtd_registros_recebidos
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
    concat(data_ingestao, '.', fonte, '.', tipo) as id,
    *
from contagem_com_complemento
order by id