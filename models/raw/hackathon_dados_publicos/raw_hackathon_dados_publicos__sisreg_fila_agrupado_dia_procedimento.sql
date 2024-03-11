{{
    config(
        alias="sisreg_fila_agrupado_dia_procedimento",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}

with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_fila_agrupado_dia_procedimento') }}
),
renamed as (
    select
        id_procedimento,
        procedimento_nome,
        safe_cast(data as date) as data,
        safe_cast(qtd_fila as int64) as qtd_fila,

    from source
)
select * from renamed