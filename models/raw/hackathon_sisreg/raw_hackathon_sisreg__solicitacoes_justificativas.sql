{{
    config(
        alias="solicitacoes_justificativas",
        schema="hackathon_sisreg",
        materialized="table",
    )
}}

with source as (
      select * from {{ source('hackathon_sisreg_staging', 'solicitacoes_justificativas') }}
),
casted as (
    select
        codigo_solicitacao,
        num_justificativa,
        justificativa,
        safe_cast(dt_justificativa as date) as dt_justificativa,
        situacao_justificativa,
        tipo_descricao,
        operador,
        nome_cnes_operador,
        lpad(cast(codigo_cnes_operador as string), 7, "0") as codigo_cnes_operador,
        tipo_perfil,
        status_solicitacao,
        sigla_situacao,
        safe_cast(data_extracao as date) as data_extracao

    from source
)
select * from casted
