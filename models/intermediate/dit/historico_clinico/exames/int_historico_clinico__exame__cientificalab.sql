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
            id,
            paciente_cpf,
            unidade as unidade_nome,
            laudo_url
        from {{ ref('raw_cientificalab__solicitacoes') }}
    ),

    exame as (
        select
            solicitacao_id,
            cod_apoio as codigo_apoio,
            data_assinatura
        from {{ ref('raw_cientificalab__exames') }}
    ),

    exames_com_resultados as (
        select
            s.paciente_cpf,
            s.unidade_nome,
            e.codigo_apoio,
            s.laudo_url,
            e.data_assinatura,
        from solicitacoes as s
        inner join exame as e on s.id = e.solicitacao_id
        where laudo_url is not null
        and paciente_cpf is not null
    ),

    exame_deduplicado as (
        select *
        from exames_com_resultados
        qualify
            row_number() over (
                partition by paciente_cpf, codigo_apoio, data_assinatura order by data_assinatura desc
            ) = 1
    )

select * from exame_deduplicado