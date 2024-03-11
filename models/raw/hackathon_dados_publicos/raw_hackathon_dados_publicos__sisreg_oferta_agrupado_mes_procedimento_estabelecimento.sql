{{
    config(
        alias="sisreg_oferta_agrupado_mes_procedimento_estabelecimento",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}


with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_oferta_agrupado_mes_procedimento_estabelecimento') }}
),
renamed as (
    select
        id_cnes,
        estabelecimento_nome,
        estabelecimento_tipo,
        safe_cast(data as date) as data,
        procedimento_esspecialidade,
        procedimento_nome,
        safe_cast(qtd_ofertada as int64) as qtd_ofertada,

    from source
)
select * from renamed
  