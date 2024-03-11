{{
    config(
        alias="sisreg_tempo_medio_espera_12_meses",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}


with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_tempo_medio_espera_12_meses') }}
),
renamed as (
    select
        procedimento_nome,
        safe_cast(ano as int64) as ano,
        safe_cast(tme_12_meses as int64) as tme_12_meses

    from source
)
select * from renamed
  