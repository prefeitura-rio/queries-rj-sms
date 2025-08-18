{{
    config(
        schema="intermediario_historico_clinico",
        alias="exames_cientificalab",
        materialized="table",
    )
}}

with
    solicitacoes as (
        select
            paciente_cpf,
            unidade,
            id
        from {{ ref('raw_cientificalab__solicitacoes') }}
    ),

    exame as (
        select
            id,
            solicitacao_id,
            cod_apoio,
            data_assinatura
        from {{ ref('raw_cientificalab__exames') }}
    ),

    resultado as (
        select
            id,
            exame_id,
            resultado,
            unidade,
            valor_referencia_minimo,
            valor_referencia_maximo,
            valor_referencia_texto
        from {{ ref('raw_cientificalab__resultados') }}
    ),

    exames_com_resultados as (
        select
            s.paciente_cpf,
            s.unidade as unidade_nome,
            e.cod_apoio,
            e.data_assinatura,
            r.resultado,
            r.unidade,
            r.valor_referencia_minimo,
            r.valor_referencia_maximo,
            r.valor_referencia_texto
        from solicitacoes as s
        inner join exame as e on s.id = e.solicitacao_id
        inner join resultado as r on e.id = r.exame_id
    ),

    exame_agg as (
        select
            paciente_cpf,
            array_agg(
                struct(
                    unidade_nome,
                    cod_apoio as codigo_do_exame,
                    data_assinatura as data_do_exame,
                    resultado,
                    unidade,
                    valor_referencia_minimo,
                    valor_referencia_maximo,
                    valor_referencia_texto
                )
            ) as exames
        from exames_com_resultados
        group by
            paciente_cpf
    )

select * from exame_agg