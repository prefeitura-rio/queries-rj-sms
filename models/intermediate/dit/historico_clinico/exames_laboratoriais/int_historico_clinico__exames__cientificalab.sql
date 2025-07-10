{{
    config(
        schema="intermediario_historico_clinico",
        alias="exames_laboratoriais_cientificalab",
        materialized="table",
    )
}}


with

    -- ===============================
    -- Tabelas Brutas
    -- ===============================
    resultados as (
        select
            *
        from
            {{ ref("raw_cientificalab__resultados") }}
    ),
    exames as (
        select
            id,
            solicitacao_id,
        from
            {{ ref("raw_cientificalab__exames") }}
    ),
    solicitacoes as (
        select
            *
        from
            {{ ref("raw_cientificalab__solicitacoes") }}
    ),


    -- ===============================
    -- Tabelas Agrupadas
    -- ===============================
    resultados_agrupados as (
        select
            exame_id,
            array_agg(
                struct(
                    cod_apoio,
                    descricao_apoio,
                    resultado,
                    unidade,
                    valor_referencia_minimo,
                    valor_referencia_maximo,
                    valor_referencia_texto
                )
            ) as resultado
        from resultados
        group by 1
    ),

    -- ===============================
    -- Exames Agrupados
    -- ===============================
    exames_agrupados as (
        select 
            exames.solicitacao_id,
            array_agg(
                struct(
                    exames.id,
                    resultados_agrupados.resultado
                )
            ) as resultados
        from exames
            inner join resultados_agrupados on exames.id = resultados_agrupados.exame_id
        group by 1
    ),

    -- ===============================
    -- Solicitacoes Agrupadas
    -- ===============================
    solicitacoes_agrupadas as (
        select
            solicitacoes.paciente_cpf,
            exames_agrupados.resultados
        from solicitacoes
            inner join exames_agrupados on solicitacoes.id = exames_agrupados.solicitacao_id
    )

select *
from solicitacoes_agrupadas