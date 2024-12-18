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
        select tipo from unnest(['patient', 'encounter', 'stock-position', 'stock-movement']) tipo
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
        select 
            id_cnes as cnes,
        from {{ref('dim_estabelecimento')}}
        where prontuario_estoque_tem_dado = 'sim' and prontuario_versao = 'vitai' 
    ),
    todos_registros_possiveis as (
        select 
            cast(unidades.cnes as string) as estabelecimento_cnes,
            cast(datas.data_atualizacao as date) as data_ingestao,
            entidades.tipo as tipo
        from datas, entidades, unidades
    ),

    -----------------------------------------
    -- ENRIQUECIMENTO
    -----------------------------------------
    -- Para converter o GID da Vitai DB em CNES
    -----------------------------------------
    vitai_db_estabelecimentos as (
        select 
            distinct
            gid as estabelecimento_gid,
            cnes as estabelecimento_cnes
        from {{ source("brutos_prontuario_vitai_staging", "m_estabelecimento_eventos") }}
    ),

    -----------------------------------------
    -- REGISTROS DE ENTIDADES RECEBIDOS
    -----------------------------------------
    -- Para identificar quais registros foram ingeridos, por entidade.
    -- Formato das tabelas: (estabelecimento_cnes, data_ingestao, tipo)
    -----------------------------------------
    estoque_posicao as (
        select 
            cnes as estabelecimento_cnes,
            safe_cast(safe_cast(_data_carga as timestamp) as date) as data_ingestao, 
            'stock-position' as tipo
        from {{ source("brutos_prontuario_vitai_staging", "estoque_posicao") }}
        {% if is_incremental() %} 
            where data_particao >= '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %} 
            where data_particao >= '{{min_date}}' 
        {% endif %}
    ),
    estoque_movimento as (
        select 
            cnes as estabelecimento_cnes,
            safe_cast(safe_cast(_data_carga as timestamp) as date) as data_ingestao, 
            'stock-movement' as tipo
        from {{ source("brutos_prontuario_vitai_staging", "estoque_movimento") }}
        {% if is_incremental() %} 
            where data_particao >= '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %} 
            where data_particao >= '{{min_date}}' 
        {% endif %}
    ),
    boletins as (
        select 
            vitai_db_estabelecimentos.estabelecimento_cnes,
            safe_cast(safe_cast(datalake__imported_at as timestamp) as date) as data_ingestao, 
            'encounter' as tipo
        from {{ source("brutos_prontuario_vitai_staging", "boletim_eventos") }}
            inner join vitai_db_estabelecimentos using (estabelecimento_gid)
        {% if is_incremental() %} 
            where data_particao >= '{{seven_days_ago}}' 
        {% endif %}
        {% if not is_incremental() %} 
            where data_particao >= '{{min_date}}' 
        {% endif %}
    ),
    pacientes as (
        select 
            vitai_db_estabelecimentos.estabelecimento_cnes,
            safe_cast(safe_cast(datalake__imported_at as timestamp) as date) as data_ingestao,
            'patient' as tipo
        from {{ source("brutos_prontuario_vitai_staging", "paciente_eventos") }}
            inner join vitai_db_estabelecimentos using (estabelecimento_gid)
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
        union all
        select *, 1 as quantidade from boletins
        union all
        select *, 1 as quantidade from estoque_posicao
        union all
        select *, 1 as quantidade from estoque_movimento
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