{{
    config(
        alias="sisreg_tempo_medio_espera_mensal",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}


with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_tempo_medio_espera_mensal') }}
),
renamed as (
    select
        procedimento_nome,
        safe_cast(data as date) as data,
        safe_cast(tme as int64) tme_mensal

    from source
)
select * from renamed
  