{{ config(
    schema = "projeto_pic",
    alias = "gestantes_puerperas",
    materialized = "table"
) }}

with

    gestacoes_em_andamento as (
        select
            cpf,
            data_diagnostico as data_inicio,
            cast(null as date) as data_fim,
            'Gestação' as fase
        from {{ ref('mart_linhas_cuidado__gestacoes') }}
        where tipo_transicao = 'Em Andamento'
    ),

    gestacoes_encerradas_em_puerperio as (
        select
            cpf,
            data_diagnostico as data_inicio,
            data_diagnostico_seguinte as data_fim,
            'Puerpério' as fase
        from {{ ref('mart_linhas_cuidado__gestacoes') }}
        where tipo_transicao = 'Encerramento Comprovado' and date_diff(current_date(), data_diagnostico_seguinte, day) <= 42
    ),

    juncao_casos as (
        select *
        from gestacoes_em_andamento
        union all
        select *
        from gestacoes_encerradas_em_puerperio
    )
select *
from juncao_casos