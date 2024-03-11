{{
    config(
        alias="sisreg_solicitacoes_agrupado_mes_procedimento_solicitante",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}


with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_solicitacoes_agrupado_mes_procedimento_solicitante') }}
),
renamed as (
    select
        id_estabelecimento_solicitante,
        estabelecimento_solicitante_nome,
        safe_cast(competencia as date) as data_competencia,
        id_procedimento,
        procedimento_nome,
        safe_cast(qtd_solicitacoes as int64) as qtd_solicitacoes,

    from source
)
select * from renamed
  