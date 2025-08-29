{{
    config(
        schema="gerenciamento__monitoramento",
        alias="estatisticas_historico_clinico_vitacare",
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
            prontuario_versao = 'vitacare' and
            prontuario_episodio_tem_dado = "sim"
    ),
    vitacare_paciente as (
        select
            payload_cnes as unidade_cnes,
            'vitacare' as fonte,
            'paciente' as tipo,
            safe_cast(safe_cast(datalake_loaded_at as timestamp) as date) as data_ingestao
        from {{ source("brutos_prontuario_vitacare_api_staging", "paciente_continuo") }}
        {% if is_incremental() %} 
        where TIMESTAMP_TRUNC(datalake_loaded_at, DAY) > '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %}
        where TIMESTAMP_TRUNC(datalake_loaded_at, DAY) >= '{{min_date}}' 
        {% endif %}
    ),
    vitacare_episodio as (
        select
            payload_cnes as unidade_cnes,
            'vitacare' as fonte,
            'episodio' as tipo,
            safe_cast(safe_cast(datalake_loaded_at as timestamp) as date) as data_ingestao
        from {{ source("brutos_prontuario_vitacare_api_staging", "atendimento_continuo") }}
        {% if is_incremental() %} 
        where TIMESTAMP_TRUNC(datalake_loaded_at, DAY) > '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %}
        where TIMESTAMP_TRUNC(datalake_loaded_at, DAY) >= '{{min_date}}' 
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
        select tipo from unnest(['paciente','episodio']) tipo
    ),
    todos_registros_possiveis as (
        select 
            cast(null as string) as unidade_cnes,
            'vitacare' as fonte, 
            entidades.tipo as tipo, 
            cast(datas.data_ingestao as date) as data_ingestao,
        from datas, entidades
    ),  
    vitacare_historico_clinico as (
        select * from vitacare_paciente
        union all
        select * from vitacare_episodio
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
        from vitacare_historico_clinico
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