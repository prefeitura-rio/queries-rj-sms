{{
    config(
        schema="gerenciamento__monitoramento",
        alias="estatisticas_farmacia",
        materialized="table",
    )
}}
with
    vitai_estoque_posicao as (
        select
            cnes as unidade_cnes,
            'vitai' as fonte,
            'posicao' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitai_staging", "estoque_posicao") }}
    ),
    vitai_estoque_movimento as (
        select
            cnes as unidade_cnes,
            'vitai' as fonte,
            'movimento' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitai_staging", "estoque_movimento") }}
    ),
    vitacare_estoque_posicao as (
        select
            cnesUnidade as unidade_cnes,
            'vitacare' as fonte,
            'posicao' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_posicao") }}
    ),
    vitacare_estoque_movimento as (
        select
            cnesUnidade as unidade_cnes,
            'vitacare' as fonte,
            'movimento' as tipo,
            data_particao as data_atualizacao
        from {{ source("brutos_prontuario_vitacare_staging", "estoque_movimento") }}
    ),
    unioned as (
        select * from vitai_estoque_posicao
            union all
        select * from vitai_estoque_movimento
            union all
        select * from vitacare_estoque_posicao
            union all
        select * from vitacare_estoque_movimento
    ),
    dinstinct as (
        select 
            distinct *
        from unioned
    ),
    grouped as (
        select 
            fonte,
            tipo,
            data_atualizacao,
            count(unidade_cnes) as quant_unidades_com_dado
        from dinstinct
        group by 1, 2, 3
    )
select * 
from grouped