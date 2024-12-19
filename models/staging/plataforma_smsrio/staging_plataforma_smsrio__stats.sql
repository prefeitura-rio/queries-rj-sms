{{
    config(
        alias="_estatisticas_ingestao",
        materialized="incremental",
        unique_key="id",
        incremental_strategy="merge",
        tags=["stats"],
    )
}}

{% set seven_days_ago = (
    modules.datetime.date.today() - modules.datetime.timedelta(days=7)
).isoformat() %}

{% set min_date = '2024-12-01' %}

with
    -----------------------------------------
    -- CRIACAO DE TODOS OS REGISTROS POSSIVEIS
    -----------------------------------------
    -- Para incluir linhas em que nenhum registro foi ingerido
    -----------------------------------------
    entidades as (
        select tipo from unnest(['patient']) tipo
    ),
    datas as (
        select data_atualizacao 
        {% if is_incremental() %}
        from unnest(GENERATE_DATE_ARRAY('{{seven_days_ago}}', current_date())) as data_atualizacao
        {% endif %}
        {% if not is_incremental() %}
        from unnest(GENERATE_DATE_ARRAY('{{min_date}}', current_date())) as data_atualizacao
        {% endif %}
    ),
    unidades as (
        select '7106513' as estabelecimento_cnes
    ),
    todos_registros_possiveis as (
        select 
            cast(unidades.cnes as string) as estabelecimento_cnes,
            cast(datas.data_atualizacao as date) as data_ingestao,
            entidades.tipo as tipo
        from datas, entidades, unidades
    ),
    -----------------------------------------
    -- REGISTROS DE ENTIDADES RECEBIDOS
    -----------------------------------------
    -- Para identificar quais registros foram ingeridos, por entidade.
    -- Formato das tabelas: (estabelecimento_cnes, data_ingestao, tipo)
    -----------------------------------------
    pacientes as (
        select 
            '7106513' as estabelecimento_cnes,
            safe_cast(safe_cast(datalake_loaded_at as timestamp) as date) as data_ingestao,
            'patient' as tipo
        from {{ source("brutos_plataforma_smsrio_staging", "_paciente_cadastro_eventos") }}
        {% if is_incremental() %} 
            where data_particao >= '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %} 
            where data_particao >= '{{min_date}}' 
        {% endif %}
    ),

    -----------------------------------------
    -- JUNÇÃO DE DADOS
    -----------------------------------------
    -- Juntando todos os registros em uma tabela única, para agrupamento por quantidade
    -----------------------------------------
    ocorrencias as (
        select *, 0 as quantidade from todos_registros_possiveis
        union all
        select *, 1 as quantidade from pacientes
    ),
    sinalizacao_transmissao as (
        select 
            tipo,
            data_ingestao,
            estabelecimento_cnes,
            sum(quantidade) as quant_registros_ingeridos
        from ocorrencias
        group by tipo, data_ingestao, estabelecimento_cnes
    ),
    identificacao as (
        select
            concat(tipo, ".", data_ingestao, ".", estabelecimento_cnes) as id, 
            *
        from sinalizacao_transmissao
    )
select *
from identificacao
order by data_ingestao, tipo, estabelecimento_cnes