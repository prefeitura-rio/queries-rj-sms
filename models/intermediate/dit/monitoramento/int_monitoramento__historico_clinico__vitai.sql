{{
    config(
        schema="gerenciamento__monitoramento",
        alias="estatisticas_historico_clinico_vitai",
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
            prontuario_versao = 'vitai' and
            prontuario_episodio_tem_dado = "sim"
    ),
    vitai_estabelecimentos as (
        select 
            gid,
            cnes
        from {{ source("brutos_prontuario_vitai_staging", "m_estabelecimento_eventos") }}
    ),
    vitai_paciente_sem_cnes as (
        select
            estabelecimento_gid,
            'vitai' as fonte,
            'paciente' as tipo,
            safe_cast(safe_cast(datalake__imported_at as timestamp) as date) as data_ingestao
        from {{ source("brutos_prontuario_vitai_staging", "paciente_eventos") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %}
        where data_particao >= '{{min_date}}' 
        {% endif %}
    ),
    vitai_episodio_sem_cnes as (
        select
            estabelecimento_gid,
            'vitai' as fonte,
            'episodio' as tipo,
            safe_cast(safe_cast(datalake__imported_at as timestamp) as date) as data_ingestao
        from {{ source("brutos_prontuario_vitai_staging", "boletim_eventos") }}
        {% if is_incremental() %} 
        where data_particao > '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %}
        where data_particao >= '{{min_date}}' 
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
            'vitai' as fonte, 
            entidades.tipo as tipo, 
            cast(datas.data_ingestao as date) as data_ingestao,
        from datas, entidades
    ),    
    vitai_historico_clinico_sem_cnes as (
        select * from vitai_paciente_sem_cnes
        union all
        select * from vitai_episodio_sem_cnes
        union all
        select * from todos_registros_possiveis
    ),
    vitai_historico_clinico as (
        select 
            vitai_estabelecimentos.cnes as unidade_cnes,
            vitai_historico_clinico_sem_cnes.* except(estabelecimento_gid),
        from vitai_historico_clinico_sem_cnes
            inner join vitai_estabelecimentos 
                on vitai_historico_clinico_sem_cnes.estabelecimento_gid = vitai_estabelecimentos.gid
    ),
    contagem as (
        select 
            data_ingestao,
            fonte,
            tipo,
            array_agg(distinct unidade_cnes ignore nulls) as unidades_com_dado,
            countif(unidade_cnes is not null) as qtd_registros_recebidos
        from vitai_historico_clinico
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