{{
    config(
        alias="sisreg_atendimentos_agrupado_dia_procedimento_estabelecimento",
        schema="hackathon_dados_publicos",
        materialized="table",
    )
}}


with source as (
      select * from {{ source('hackathon_dados_publicos_staging', 'sisreg_atendimentos_agrupado_dia_procedimento_estabelecimento') }}
),
renamed as (
    select
        id_cnes as id_estabelecimento,
        estabelecimento_nome,
        estabelecimento_tipo,
        safe_cast(data as date) as data,
        procedimento_especialidade,
        procedimento_nome,
        safe_cast(qtd_executada as int64) as qtd_executada,

    from source
)
select * from renamed
