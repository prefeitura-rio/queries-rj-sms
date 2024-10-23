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

with
    unidades_esperadas as (
        select 
            area_programatica as unidade_ap,
            id_cnes as unidade_cnes,
            nome_limpo as unidade_nome
        from {{ref('dim_estabelecimento')}}
        where prontuario_versao = 'vitacare' 
    ),
    vitacare_paciente as (
        select
            payload_cnes as unidade_cnes,
            'vitacare' as fonte,
            'paciente' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitacare_staging", "paciente_eventos") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    vitacare_episodio as (
        select
            payload_cnes as unidade_cnes,
            'vitacare' as fonte,
            'episodio' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitacare_staging", "atendimento_eventos") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
    ),
    datas as (
        select data_atualizacao 
        {% if is_incremental() %}
        from unnest(GENERATE_DATE_ARRAY('{{seven_days_ago}}', current_date())) as data_atualizacao
        {% endif %}
        {% if not is_incremental() %}
        from unnest(GENERATE_DATE_ARRAY('2015-01-01', current_date())) as data_atualizacao
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
            cast(datas.data_atualizacao as string) as data_atualizacao,
        from datas, entidades
    ),  
    vitacare_historico_clinico as (
        select * from vitacare_paciente
        union all
        select * from vitacare_episodio
    ),
    contagem as (
        select 
            data_atualizacao,
            fonte,
            tipo,
            array_agg(distinct unidade_cnes) as unidades_com_dado,
            count(*) as qtd_registros_recebidos
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
    concat(data_atualizacao, '.', fonte, '.', tipo) as id,
    *
from contagem_com_complemento
order by id